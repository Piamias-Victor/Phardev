from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from uuid import UUID
import logging
import traceback

import dateutil.parser
from django.db import transaction
from django.db.models import OuterRef, Subquery, F, Window
from django.db.models.functions import RowNumber
from django.utils import timezone
from tqdm import tqdm
import pytz

from data.models import GlobalProduct, InternalProduct, ProductOrder, Supplier, Order, Sales, InventorySnapshot

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


def process_product_winpharma(pharmacy, data):
    """
    Process WinPharma product data to create or update global products, internal products, and inventory snapshots.

    Args:
        pharmacy: Pharmacy instance associated with the data.
        data: List of dictionaries representing product data.

    Returns:
        A dictionary containing lists of created products and snapshots.
    """
    preprocessed_data = []
    for obj in data:
        try:
            if int(obj.get('id')) < 0:
                continue
            tva_value = float(obj.get('TVA', 0.0))
            if tva_value > 1:
                tva_converted = Decimal(tva_value / 100)
            else:
                tva_converted = Decimal(tva_value)

            # Cap prices to a maximum value
            preprocessed_data.append({
                'product_id': str(obj.get('id')),
                'name': obj.get('nom', ''),
                'code_13_ref': obj.get('code13Ref') or None,
                'TVA': tva_converted,
                'stock': clamp(int(obj.get('stock', 0)), -32768, 32767),
                'price_with_tax': min(Decimal(obj.get('prixTtc', 0)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP),
                                      Decimal('99999999.99')),
                'weighted_average_price': min(
                    Decimal(obj.get('prixMP', 0)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP),
                    Decimal('99999999.99')),
            })

        except (ValueError, TypeError) as e:
            logger.warning(f"Error preprocessing data for product {obj.get('id', 'unknown')}: {e}")
            continue

    # Collect unique GlobalProduct references
    code_13_refs = {obj['code_13_ref'] for obj in preprocessed_data if obj['code_13_ref']}

    # Retrieve existing GlobalProduct instances
    with transaction.atomic():
        existing_global_products = GlobalProduct.objects.filter(code_13_ref__in=code_13_refs).only('code_13_ref',
                                                                                                   'name')
        global_products_map = {gp.code_13_ref: gp for gp in existing_global_products}

        # Identify missing references and create them
        missing_refs = code_13_refs - global_products_map.keys()
        if missing_refs:
            GlobalProduct.objects.bulk_create(
                [GlobalProduct(code_13_ref=ref, name='Default Name') for ref in missing_refs]
            )
            # Update the map with newly created GlobalProducts
            new_global_products = GlobalProduct.objects.filter(code_13_ref__in=missing_refs)
            global_products_map.update({gp.code_13_ref: gp for gp in new_global_products})

    # Prepare data for InternalProduct
    internal_products_data = [
        {
            'pharmacy_id': pharmacy.id,
            'internal_id': obj['product_id'],
            'code_13_ref': global_products_map.get(obj['code_13_ref'], None),
            'name': obj['name'],
            'TVA': obj['TVA'],
        }
        for obj in preprocessed_data
    ]

    # Create or update InternalProduct instances
    try:
        products = bulk_process(
            model=InternalProduct,
            data=internal_products_data,
            unique_fields=['pharmacy_id', 'internal_id'],
            update_fields=['code_13_ref', 'name', 'TVA']
        )
    except Exception as e:
        logger.error(f"Error during bulk process of products: {e}")
        raise

    # Map InternalProduct instances by internal_id
    products_map = {str(product.internal_id): product for product in products}

    # Retrieve the latest InventorySnapshot for each product
    latest_snapshots = InventorySnapshot.objects.filter(
        product_id__in=[product.id for product in products]
    ).annotate(
        row_number=Window(
            expression=RowNumber(),
            partition_by=F('product_id'),
            order_by=F('date').desc()
        )
    ).filter(row_number=1).values('product_id', 'stock', 'price_with_tax', 'weighted_average_price')

    # Create a mapping of the latest snapshots by internal_id
    latest_snapshots_map = {snapshot['product_id']: snapshot for snapshot in latest_snapshots}

    # Prepare data for InventorySnapshot
    inventory_snapshots_data = []
    for obj in preprocessed_data:
        product_id = obj['product_id']
        product = products_map.get(product_id)

        # Retrieve the latest snapshot for this product
        last_snapshot = latest_snapshots_map.get(product.id)
        needs_update = (
                not last_snapshot or
                last_snapshot['stock'] != obj['stock'] or
                last_snapshot['price_with_tax'] != obj['price_with_tax'] or
                last_snapshot['weighted_average_price'] != obj['weighted_average_price']
        )

        if needs_update:
            inventory_snapshots_data.append({
                'product_id': product.id,
                'stock': obj['stock'],
                'price_with_tax': obj['price_with_tax'],
                'weighted_average_price': obj['weighted_average_price'],
                'date': date.today()
            })

    # Bulk create InventorySnapshot instances
    if inventory_snapshots_data:
        bulk_process(
            model=InventorySnapshot,
            data=inventory_snapshots_data,
            unique_fields=['product_id', 'date'],
            update_fields=['stock', 'price_with_tax', 'weighted_average_price']
        )


def process_order_winpharma(pharmacy, data):
    """
    Process WinPharma order data to create or update suppliers, products, and orders in the database.

    Args:
        pharmacy: Pharmacy instance associated with the data.
        data: List of dictionaries representing Winpharma order data.

    Returns:
        dict: A dictionary containing lists of created suppliers, products, orders, and product-order associations.
    """
    preprocessed_data = []
    for obj in data:
        try:
            # Clean and validate order data
            order_id = obj.get('idCmd')
            order_id = int(order_id.split('-')[0]) if isinstance(order_id, str) else int(order_id)

            supplier_id = obj.get('codeFourn')
            if not supplier_id:
                raise ValueError("Missing 'codeFourn'")
            supplier_name = obj.get('nomFourn', obj.get('code_supplier', ''))
            step = obj.get('etape', '')
            sent_date = parse_date(obj.get('envoi'))
            delivery_date = parse_date(obj.get('dateLivraison'), False)

            products = []
            for line in obj.get('produits', []):
                try:
                    if int(line.get('prodId')) < 0:
                        continue

                    product_id = str(line['prodId'])
                    qte = clamp(int(line.get('qte', 0)), -32768, 32767)  # SmallIntegerField
                    qte_r = clamp(int(line.get('qteR', 0)), -32768, 32767)  # SmallIntegerField
                    qte_a = clamp(int(line.get('qteA', 0)), -32768, 32767)  # SmallIntegerField
                    qte_ug = clamp(int(line.get('qteUG', 0)), -32768, 32767)  # SmallIntegerField
                    qte_ec = clamp(int(line.get('qteEC', 0)), -32768, 32767)  # SmallIntegerField
                    qte_ar = clamp(int(line.get('qteAReceptionner', 0)), -32768, 32767)  # SmallIntegerField

                    products.append({
                        'product_id': product_id,
                        'qte': qte,
                        'qteUG': qte_ug,
                        'qteA': qte_a,
                        'qteR': qte_r,
                        'qteAReceptionner': qte_ar,
                        'qteEC': qte_ec
                    })
                except (ValueError, TypeError, KeyError) as e:
                    logger.warning(f"Error preprocessing product in order {order_id}: {e}")
                    continue

            preprocessed_data.append({
                'order_id': order_id,
                'supplier_id': supplier_id,
                'supplier_name': supplier_name,
                'step': step,
                'sent_date': sent_date,
                'delivery_date': delivery_date,
                'products': products
            })
        except (ValueError, TypeError) as e:
            logger.warning(f"Error preprocessing order {obj.get('idCmd', 'unknown')}: {e}")
            continue

    # Collect unique supplier codes and prepare supplier data
    supplier_data = []
    seen_suppliers = set()

    for obj in preprocessed_data:
        supplier_key = (pharmacy.id, obj['supplier_id'])
        if supplier_key not in seen_suppliers:
            supplier_data.append({
                'pharmacy_id': pharmacy.id,
                'code_supplier': obj['supplier_id'],
                'name': obj['supplier_name']
            })
            seen_suppliers.add(supplier_key)

    # Create or update suppliers
    try:
        suppliers = bulk_process(
            model=Supplier,
            data=supplier_data,
            unique_fields=['pharmacy_id', 'code_supplier'],
            update_fields=['name']
        )
    except Exception as e:
        logger.error(f"Error processing suppliers: {e}")
        raise

    # Map suppliers by their codes for easy access
    suppliers_map = {supplier.code_supplier: supplier for supplier in suppliers}

    # Extract all product IDs from the order lines
    product_ids = {line['product_id'] for obj in preprocessed_data for line in obj.get('products', [])}

    # Retrieve existing InternalProduct instances for the pharmacy
    existing_products = InternalProduct.objects.filter(
        pharmacy_id=pharmacy.id,
        internal_id__in=product_ids
    )

    # Prepare data for InternalProduct
    internal_product_data = [
        {
            'internal_id': product.internal_id,
            'pharmacy_id': pharmacy.id,
            'name': product.name  # Retain existing name
        }
        for product in existing_products
    ]

    # Identify and prepare data for new products
    new_product_ids = product_ids - set(existing_products.values_list('internal_id', flat=True))
    internal_product_data.extend([
        {
            'internal_id': product_id,
            'pharmacy_id': pharmacy.id,
            'name': 'empty'  # Assign default name to new products
        }
        for product_id in new_product_ids
    ])

    # Create or update internal products
    try:
        products = bulk_process(
            model=InternalProduct,
            data=internal_product_data,
            unique_fields=['pharmacy_id', 'internal_id'],
            update_fields=['name']
        )
    except Exception as e:
        logger.error(f"Error processing internal products: {e}")
        raise

    # Map InternalProduct instances by their internal_id for easy access
    internal_products_map = {product.internal_id: product for product in products}

    # Prepare order data for bulk processing
    order_data = [
        {
            'internal_id': obj['order_id'],
            'pharmacy_id': pharmacy.id,
            'supplier_id': suppliers_map[obj['supplier_id']].id,
            'step': obj['step'],
            'sent_date': obj['sent_date'],
            'delivery_date': obj['delivery_date'],
        }
        for obj in preprocessed_data
    ]

    # Create or update orders
    try:
        orders = bulk_process(
            model=Order,
            data=order_data,
            unique_fields=['internal_id', 'pharmacy_id'],
            update_fields=['supplier_id', 'step', 'sent_date', 'delivery_date']
        )
    except Exception as e:
        logger.error(f"Error processing orders: {e}")
        raise

    # Map Order instances by their internal_id for easy access
    order_map = {order.internal_id: order for order in orders}

    # Prepare ProductOrder data for bulk processing
    product_order_data = []
    for obj in preprocessed_data:
        internal_id = obj['order_id']
        for line in obj['products']:
            product = internal_products_map.get(line['product_id'])
            order = order_map.get(internal_id)
            if not product or not order:
                continue

            product_order_data.append({
                'product': product,
                'order': order,
                'qte': line['qte'],
                'qte_ug': line['qteUG'],
                'qte_a': line['qteA'],
                'qte_r': line['qteR'],
                'qte_ar': line['qteAReceptionner'],
                'qte_ec': line['qteEC']
            })

    # Create or update ProductOrder instances
    try:
        bulk_process(
            model=ProductOrder,
            data=product_order_data,
            unique_fields=['order', 'product'],
            update_fields=['qte', 'qte_r', 'qte_a', 'qte_ug', 'qte_ec', 'qte_ar']
        )
    except Exception as e:
        logger.error(f"Error processing product-order associations: {e}")
        raise


def process_sales_winpharma(pharmacy, data):
    """
    Process sales records for a pharmacy and its products.

    Args:
        pharmacy: Pharmacy instance associated with the data.
        data: List of dictionaries representing sales data.

    Returns:
        None
    """
    preprocessed_data = []
    for obj in data:
        try:
            if int(obj.get('prodId')) < 0:
                continue
            # Clean and validate sales data
            preprocessed_data.append({
                'product_id': str(obj['prodId']),
                'date': obj.get('heure'),
                'qte': int(obj.get('qte', 0))
            })
        except (ValueError, TypeError, KeyError) as e:
            logger.warning(f"Error preprocessing sale record {obj.get('prodId', 'unknown')}: {e}")
            continue

    # Prepare a set of product IDs to process
    product_ids = {obj['product_id'] for obj in preprocessed_data}

    latest_snapshots = (
        InventorySnapshot.objects
        .filter(product=OuterRef('id'))
        .order_by('-created_at')
        .values('id')[:1])
    internal_products = InternalProduct.objects.filter(pharmacy=pharmacy, internal_id__in=product_ids).annotate(
        latest_snapshot_id=Subquery(latest_snapshots)
    )
    internal_products_map = {str(product.internal_id): product.latest_snapshot_id for product in internal_products}

    aggregated_sales = {}
    for obj in preprocessed_data:
        product_id = obj['product_id']
        if product_id not in aggregated_sales:
            aggregated_sales[product_id] = (obj['date'], obj['qte'])
        else:
            old = aggregated_sales[product_id]
            new = (old[0], old[1] + obj['qte'])
            aggregated_sales[product_id] = new

    sales_data = []
    for key, value in aggregated_sales.items():

        snapshot_id = internal_products_map.get(key)
        if not snapshot_id:
            continue
        sales_data.append({
            'product_id': snapshot_id,
            'quantity': clamp(int(value[1]), -32768, 32767),
            'date': parse_date(value[0], False),
        })

    # Process sales in bulk
    try:
        bulk_process(
            model=Sales,
            data=sales_data,
            unique_fields=['product_id', 'date'],
            update_fields=['quantity']
        )
    except Exception as e:
        logger.error(f"Error processing sales: {e}")
        raise


def process_stock_dexter(pharmacy, data, date_str):
    """
    Creates or updates products and their inventory snapshots using bulk_process.

    Args:
        pharmacy: Pharmacy instance associated with the data.
        data: List of dictionaries representing stock data.
        date_str (str): Date string representing the snapshot date.

    Returns:
        A dictionary containing lists of created products and snapshots.
    """
    preprocessed_data = []
    for obj in data:
        try:
            if int(obj.get('produit_id')) < 0:
                continue
            code_13_ref = None
            for code in obj.get('code_produit') or []:
                if code.get('referent'):
                    code_13_ref = code.get('code')
                    break

            tva_value = float(obj['taux_Tva']) if obj.get('taux_Tva') is not None else 0.0
            if tva_value > 1:
                tva_converted = Decimal(tva_value / 100)
            else:
                tva_converted = Decimal(tva_value)

            preprocessed_data.append({
                'product_id': str(obj['produit_id']),
                'name': str(obj['libelle_produit']) if obj.get('libelle_produit') else "",
                'code_13_ref': code_13_ref or "",
                'TVA': tva_converted,
                'stock': clamp(int(obj['qte_stock']) if obj.get('qte_stock') else 0, -32768, 32767),
                'weighted_average_price': Decimal(str(obj['px_achat_PMP_HT'])).quantize(Decimal('0.01'),
                                                                                        rounding=ROUND_HALF_UP) if obj.get(
                    'px_achat_PMP_HT') else Decimal('0.00'),
                'price_with_tax': Decimal(str(obj['px_vte_TTC'])).quantize(Decimal('0.01'),
                                                                           rounding=ROUND_HALF_UP) if obj.get(
                    'px_vte_TTC') else Decimal('0.00')
            })

        except (ValueError, TypeError) as e:
            logger.warning(
                f"Error preprocessing data for product {obj.get('produit_id', 'unknown')}: {traceback.format_exc()}")
            continue

    # Collect unique GlobalProduct references
    code_13_refs = {obj['code_13_ref'] for obj in preprocessed_data if obj['code_13_ref']}

    # Retrieve existing GlobalProduct instances
    with transaction.atomic():
        existing_global_products = GlobalProduct.objects.filter(code_13_ref__in=code_13_refs).only('code_13_ref',
                                                                                                   'name')
        global_products_map = {gp.code_13_ref: gp for gp in existing_global_products}

        # Identify missing references and create them
        missing_refs = code_13_refs - global_products_map.keys()
        if missing_refs:
            GlobalProduct.objects.bulk_create(
                [GlobalProduct(code_13_ref=ref, name='Default Name') for ref in missing_refs]
            )
            # Update the map with newly created GlobalProducts
            new_global_products = GlobalProduct.objects.filter(code_13_ref__in=missing_refs)
            global_products_map.update({gp.code_13_ref: gp for gp in new_global_products})

    # Prepare data for InternalProduct
    internal_products_data = [
        {
            'pharmacy_id': pharmacy.id,
            'internal_id': obj['product_id'],
            'code_13_ref': global_products_map.get(obj['code_13_ref'], None),
            'name': obj['name'],
            'TVA': obj['TVA'],

        }
        for obj in preprocessed_data
    ]

    # Create or update InternalProduct instances
    try:
        products = bulk_process(
            model=InternalProduct,
            data=internal_products_data,
            unique_fields=['pharmacy_id', 'internal_id'],
            update_fields=['code_13_ref', 'name', 'TVA']
        )
    except Exception as e:
        logger.error(f"Error during bulk process of products: {e}")
        raise

    # Map created/updated InternalProduct instances by internal_id
    products_map = {str(product.internal_id): product for product in products}

    # Parse the snapshot date
    snapshot_date = parse_date(date_str, is_datetime=False)

    # Retrieve the latest InventorySnapshot for each product
    latest_snapshots = InventorySnapshot.objects.filter(
        product_id__in=[product.id for product in products]
    ).annotate(
        row_number=Window(
            expression=RowNumber(),
            partition_by=F('product_id'),
            order_by=F('date').desc()
        )
    ).filter(row_number=1).values('product_id', 'stock', 'price_with_tax', 'weighted_average_price')

    # Create a mapping of the latest snapshots by internal_id
    latest_snapshots_map = {snapshot['product_id']: snapshot for snapshot in latest_snapshots}

    # Prepare data for InventorySnapshot
    inventory_snapshots_data = []
    for obj in preprocessed_data:
        product_id = obj['product_id']
        product = products_map.get(product_id)

        # Retrieve the latest snapshot for this product
        last_snapshot = latest_snapshots_map.get(product.id)

        needs_update = (
                not last_snapshot or
                last_snapshot['stock'] != obj['stock'] or
                last_snapshot['price_with_tax'] != obj['price_with_tax'] or
                last_snapshot['weighted_average_price'] != obj['weighted_average_price']
        )

        if needs_update:
            inventory_snapshots_data.append({
                'product_id': product.id,
                'stock': obj['stock'],
                'price_with_tax': obj['price_with_tax'],
                'weighted_average_price': obj['weighted_average_price'],
                'date': snapshot_date
            })

    # Bulk create InventorySnapshot instances
    if inventory_snapshots_data:
        bulk_process(
            model=InventorySnapshot,
            data=inventory_snapshots_data,
            unique_fields=['product_id', 'date'],
            update_fields=['stock', 'price_with_tax', 'weighted_average_price']
        )


def process_achat_dexter(pharmacy, data):
    """
    Creates or updates orders and their associated product lines using bulk_process.

    Args:
        pharmacy: Pharmacy instance to associate with the data.
        data: List of dictionaries representing Dexter order data.

    Returns:
        dict: A dictionary containing lists of created suppliers, products, orders, and product-order associations.
    """
    preprocessed_data = []
    for obj in data:
        try:
            # Clean and validate order data
            order_id = str(obj['commande_id'])
            supplier_id = str(obj['id_fournisseur'])
            supplier_name = str(obj.get('libelle_fournisseur', obj.get('id_fournisseur', '')))
            step = str(obj.get('etat_commande', ''))
            sent_date = parse_date(obj.get('date_transmission'))
            delivery_date = parse_date(obj.get('date_reception'), is_datetime=False)

            products = []
            for line in obj.get('lignes', []):
                try:
                    if int(line.get('produit_id')) < 0:
                        continue

                    product_id = str(line['produit_id'])
                    qte_cde = int(line.get('qte_cde', 0))
                    total_recu = int(line.get('total_recu', 0))
                    total_ug_liv = int(line.get('total_ug_liv', 0))

                    products.append({
                        'product_id': product_id,
                        'qte_cde': qte_cde,
                        'total_recu': total_recu,
                        'total_ug_liv': total_ug_liv
                    })
                except (ValueError, TypeError, KeyError) as e:
                    logger.warning(f"Error preprocessing product line in order {order_id}: {e}")
                    continue

            preprocessed_data.append({
                'order_id': order_id,
                'supplier_id': supplier_id,
                'supplier_name': supplier_name,
                'step': step,
                'sent_date': sent_date,
                'delivery_date': delivery_date,
                'products': products
            })
        except (ValueError, TypeError, KeyError) as e:
            logger.warning(f"Error preprocessing order {obj.get('commande_id', 'unknown')}: {e}")
            continue

    # Collect unique supplier codes and prepare supplier data
    supplier_data = []
    seen_suppliers = set()

    for obj in preprocessed_data:
        supplier_key = (pharmacy.id, obj['supplier_id'])
        if supplier_key not in seen_suppliers:
            supplier_data.append({
                'pharmacy_id': pharmacy.id,
                'code_supplier': obj['supplier_id'],
                'name': obj['supplier_name']
            })
            seen_suppliers.add(supplier_key)

    # Create or update suppliers
    try:
        suppliers = bulk_process(
            model=Supplier,
            data=supplier_data,
            unique_fields=['pharmacy_id', 'code_supplier'],
            update_fields=['name']
        )
    except Exception as e:
        logger.error(f"Error processing suppliers: {e}")
        raise

    # Map suppliers for quick lookup
    suppliers_map = {supplier.code_supplier: supplier for supplier in suppliers}

    # Extract and prepare internal product data
    product_ids = {line['product_id'] for obj in preprocessed_data for line in obj['products']}

    existing_products = InternalProduct.objects.filter(
        pharmacy_id=pharmacy.id,
        internal_id__in=product_ids
    )
    internal_product_data = [
        {
            'internal_id': product.internal_id,
            'pharmacy_id': pharmacy.id,
            'name': product.name  # Retain existing name
        }
        for product in existing_products
    ]

    # Identify and prepare data for new products
    new_product_ids = product_ids - set(existing_products.values_list('internal_id', flat=True))
    internal_product_data.extend([
        {
            'internal_id': product_id,
            'pharmacy_id': pharmacy.id,
            'name': 'empty'  # Assign default name to new products
        }
        for product_id in new_product_ids
    ])

    # Create or update internal products
    try:
        products = bulk_process(
            model=InternalProduct,
            data=internal_product_data,
            unique_fields=['pharmacy_id', 'internal_id'],
            update_fields=['name']
        )
    except Exception as e:
        logger.error(f"Error processing internal products: {e}")
        raise

    # Map InternalProduct instances by their internal_id for easy access
    internal_products_map = {str(product.internal_id): product for product in products}

    # Prepare order data for bulk processing
    order_data = []
    for obj in preprocessed_data:
        try:
            order_data.append({
                'internal_id': obj['order_id'],
                'pharmacy_id': pharmacy.id,
                'supplier_id': suppliers_map[obj['supplier_id']].id,
                'step': obj['step'],
                'sent_date': obj['sent_date'],
                'delivery_date': obj['delivery_date'],
            })
        except KeyError as e:
            logger.warning(f"Missing key in order {obj}: {e}")
            continue
        except Exception as e:
            logger.error(f"Error processing order {obj}: {e}")
            continue

    # Create or update orders
    try:
        orders = bulk_process(
            model=Order,
            data=order_data,
            unique_fields=['internal_id', 'pharmacy_id'],
            update_fields=['supplier_id', 'step', 'sent_date', 'delivery_date']
        )
    except Exception as e:
        logger.error(f"Error processing orders: {e}")
        raise

    # Map orders for quick lookup
    order_map = {str(order.internal_id): order for order in orders}

    # Prepare ProductOrder data for bulk processing
    product_order_data = []
    existing_associations = set()  # Set to keep track of existing associations

    for obj in preprocessed_data:
        internal_id = obj['order_id']
        order = order_map.get(internal_id)
        if not order:
            logger.warning(f"Order with internal_id {internal_id} not found.")
            continue

        for line in obj['products']:
            product_id = line['product_id']
            product = internal_products_map.get(product_id)
            if not product:
                logger.warning(f"InternalProduct with internal_id {product_id} not found.")
                continue

            # Ensure quantities are integers
            qte = clamp(int(line.get('qte_cde', 0)), -32768, 32767)  # SmallIntegerField
            qte_a = clamp(int(line.get('qte_cde', 0)), -32768, 32767)  # SmallIntegerField
            qte_ug = clamp(int(line.get('total_ug_liv', 0)), -32768, 32767)  # SmallIntegerField
            qte_r = clamp(int(line.get('total_recu', 0)), -32768, 32767)  # SmallIntegerField

            # Calcul des valeurs et clamping
            qte_ec = clamp(qte - qte_r, -32768, 32767)  # Écart entre commandé et reçu
            qte_ar = clamp(qte - qte_r, -32768, 32767)  # Quantité à réceptionner

            # Create a unique key for the association
            association_key = (order.id, product.id)
            if association_key in existing_associations:
                continue  # Skip if association already exists

            # Add to the list and mark as seen
            existing_associations.add(association_key)

            product_order_data.append({
                'product': product,
                'order': order,
                'qte': qte,
                'qte_a': qte_a,
                'qte_ug': qte_ug,
                'qte_r': qte_r,
                'qte_ec': qte_ec,
                'qte_ar': qte_ar
            })

    # Create or update ProductOrder instances
    try:
        bulk_process(
            model=ProductOrder,
            data=product_order_data,
            unique_fields=['order', 'product'],
            update_fields=['qte', 'qte_r', 'qte_a', 'qte_ug', 'qte_ec', 'qte_ar']
        )
    except Exception as e:
        logger.error(f"Error processing product-order associations: {e}")
        raise


def process_vente_dexter(pharmacy, data):
    """
    Creates or updates sales and their associated product lines using bulk_process.

    Args:
        pharmacy: Pharmacy instance to associate with the data.
        data: List of dictionaries representing Dexter sales data.

    Returns:
        dict: A dictionary containing lists of created products, sales, and product-order associations.
    """
    preprocessed_data = []
    for obj in data:
        try:
            # Extract and validate sales data
            for invoice in obj.get('factures', []):
                for line in invoice.get('lignes_de_facture', []):
                    product_id = line.get('produit_id')
                    if not product_id:
                        continue

                    preprocessed_data.append({
                        'product_id': str(product_id),
                        'qte': int(line.get('quantite', 0)),
                        'date': obj.get('date_acte'),
                    })
        except (ValueError, TypeError, KeyError) as e:
            logger.warning(f"Error preprocessing sales data for object {obj.get('commande_id', 'unknown')}: {e}")
            continue

    # Prepare a set of product IDs to process
    product_ids = {obj['product_id'] for obj in preprocessed_data}

    latest_snapshots = (
        InventorySnapshot.objects
        .filter(product=OuterRef('id'))
        .order_by('-created_at')
        .values('id')[:1])
    internal_products = InternalProduct.objects.filter(pharmacy=pharmacy, internal_id__in=product_ids).annotate(
        latest_snapshot_id=Subquery(latest_snapshots)
    )
    internal_products_map = {str(product.internal_id): product.latest_snapshot_id for product in internal_products}

    aggregated_sales = {}
    for obj in preprocessed_data:
        product_id = obj['product_id']
        day = parse_date(obj['date'], is_datetime=False)
        key = (product_id, day)
        if key not in aggregated_sales:
            aggregated_sales[key] = obj['qte']
        else:
            aggregated_sales[key] += obj['qte']

    sales_data = []
    for (product_id, day), total_qte in aggregated_sales.items():
        snapshot_id = internal_products_map.get(product_id)
        if not snapshot_id:
            continue
        if total_qte > 10000:
            print(f"Quantité élevée le {day} pour produit {product_id}: {total_qte}")
        sales_data.append({
            'product_id': snapshot_id,
            'quantity': clamp(total_qte, -32768, 32767),
            'date': day,
        })

    # Process sales in bulk
    try:
        bulk_process(
            model=Sales,
            data=sales_data,
            unique_fields=['product_id', 'date'],
            update_fields=['quantity']
        )
    except Exception as e:
        logger.error(f"Error processing sales: {e}")
        raise
