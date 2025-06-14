from decimal import Decimal
from uuid import UUID
import logging

import dateutil.parser
from django.db import transaction
from django.utils import timezone
from tqdm import tqdm
import pytz

logger = logging.getLogger(__name__)


def clamp(value, min_value, max_value):
    return max(min_value, min(value, max_value))


def chunked_iterable(iterable, chunk_size):
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i:i + chunk_size]


def parse_date(date_str, is_datetime=True):
    """
    Converts a date string into a timezone-aware datetime object or a date object.

    Args:
        date_str (str): The date string to parse.
        is_datetime (bool): Whether to parse as datetime (True) or date (False).

    Returns:
        datetime.datetime or datetime.date or None: Parsed datetime/date object or None if parsing fails.
    """
    if not date_str:
        return None

    try:
        parsed_date = dateutil.parser.parse(date_str)

        if is_datetime:
            if timezone.is_naive(parsed_date):
                parsed_date = timezone.make_aware(parsed_date, pytz.UTC)
            else:
                parsed_date = parsed_date.astimezone(pytz.UTC)
            return parsed_date

        else:
            return parsed_date.date()
    except (ValueError, TypeError) as e:
        logger.warning(f"Error parsing date '{date_str}': {e}")
        return None


def bulk_process(model, data, unique_fields, update_fields, chunk_size=1000):
    """
    Efficiently creates or updates a batch of objects in the database.

    Args:
        model: Django model to operate on (e.g., MyModel).
        data: List of dictionaries representing the objects to process.
        unique_fields: Fields that uniquely identify each object.
        update_fields: Fields to update for existing objects.
        chunk_size: Number of objects to process per batch (default: 1000).

    Returns:
        List of all created and updated objects.

    Raises:
        ValueError: If the number of processed objects doesn't match the input data.
    """
    # Build filters to identify existing objects
    filters = {
        f"{field}__in": [item[field] for item in data]
        for field in unique_fields
    }

    def normalize_value(value):
        """Convert values to a consistent format for comparison."""
        if isinstance(value, UUID):
            return str(value)
        elif isinstance(value, Decimal):
            return round(value, 2)
        return str(value)

    # Retrieve existing objects based on unique fields
    existing_objects = model.objects.filter(**filters)

    # Create a mapping of unique keys to existing objects
    existing_objects_map = {
        tuple(normalize_value(getattr(obj, field)) for field in unique_fields): obj
        for obj in existing_objects
    }

    objects_to_update = []
    objects_to_create = []

    for item in tqdm(data, desc=f"Processing data {model}"):
        # Generate a unique key for the current item
        lookup_key = tuple(normalize_value(item.get(field)) for field in unique_fields)
        if lookup_key in existing_objects_map:
            objects_to_update.append(existing_objects_map[lookup_key])
        else:
            objects_to_create.append(model(**item))

    all_objects = []

    # Bulk create new objects in chunks
    for chunk in chunked_iterable(objects_to_create, chunk_size):
        with transaction.atomic():
            created = model.objects.bulk_create(chunk)
            all_objects.extend(created)

    # Bulk update existing objects in chunks
    for chunk in chunked_iterable(objects_to_update, chunk_size):
        with transaction.atomic():
            model.objects.bulk_update(chunk, fields=update_fields)
            all_objects.extend(chunk)

    if len(all_objects) != len(data):
        raise ValueError("Mismatch between input data and processed objects count.")

    print(f"Created: {len(objects_to_create)}, Updated: {len(objects_to_update)}")

    return all_objects
