from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from uuid import UUID
import logging

import dateutil.parser
from django.db import transaction
from django.db.models import OuterRef, Subquery, F, Window
from django.db.models.functions import RowNumber
from django.utils import timezone
from tqdm import tqdm
import pytz

from data.models import GlobalProduct, InternalProduct, ProductOrder, Supplier, Order, Sales, InventorySnapshot

logger = logging.getLogger(__name__)


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
            code_13_ref = obj.get('code13Ref') or None
            nom = obj.get('nom', '')
            tva = float(obj.get('TVA', 0.0))
            stock = int(obj.get('stock', 0))
            prix_ttc = Decimal(obj.get('prixTtc', 0)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            prix_mp = Decimal(obj.get('prixMP', 0)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

            # Cap prices to a maximum value
            prix_ttc = min(prix_ttc, Decimal('99999999.99'))
            prix_mp = min(prix_mp, Decimal('99999999.99'))

            preprocessed_data.append({
                'id': str(obj.get('id')),
                'code13Ref': code_13_ref,
                'nom': nom,
                'TVA': tva,
                'stock': stock,
                'prixTtc': prix_ttc,
                'prixMP': prix_mp,
            })
        except (ValueError, TypeError) as e:
            logger.warning(f"Error preprocessing data for product {obj.get('id', 'unknown')}: {e}")
            continue

    # Collect unique GlobalProduct references
    code_13_refs = {obj['code13Ref'] for obj in preprocessed_data if obj['code13Ref']}

    # Retrieve existing GlobalProduct instances
    with transaction.atomic():
        existing_global_products = GlobalProduct.objects.filter(code_13_ref__in=code_13_refs).only('code_13_ref', 'name')
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
            'internal_id': obj['id'],
            'code_13_ref': global_products_map.get(obj['code13Ref']),
            'name': obj['nom'],
            'TVA': obj['TVA'],
        }
        for obj in preprocessed_data
    ]

    # Create or update InternalProduct instances
    products = bulk_process(
        model=InternalProduct,
        data=internal_products_data,
        unique_fields=['pharmacy_id', 'internal_id'],
        update_fields=['code_13_ref', 'name', 'TVA']
    )

    # Map InternalProduct instances by internal_id
    products_map = {str(product.internal_id): product for product in products}
    product_ids = [product.id for product in products]

    # Retrieve the latest InventorySnapshot for each product
    latest_snapshots = InventorySnapshot.objects.filter(product_id__in=product_ids).annotate(
        row_number=Window(
            expression=RowNumber(),
            partition_by=F('product_id'),
            order_by=F('date').desc()
        )
    ).filter(row_number=1).values('product_id', 'stock', 'price_with_tax', 'weighted_average_price')
    latest_snapshots_map = {snapshot['product_id']: snapshot for snapshot in latest_snapshots}

    # Prepare InventorySnapshot data
    inventory_snapshots_to_create = []
    for obj in preprocessed_data:
        internal_id = obj['id']
        product = products_map.get(internal_id)
        if not product:
            continue

        last_snapshot = latest_snapshots_map.get(product.id)
        needs_update = (
            not last_snapshot or
            last_snapshot['stock'] != obj['stock'] or
            last_snapshot['price_with_tax'] != obj['prixTtc'] or
            last_snapshot['weighted_average_price'] != obj['prixMP']
        )

        if needs_update:
            inventory_snapshots_to_create.append({
                'product_id': product.id,
                'stock': obj['stock'],
                'price_with_tax': obj['prixTtc'],
                'weighted_average_price': obj['prixMP'],
                'date': date.today()
            })

    # Bulk create InventorySnapshot instances
    if inventory_snapshots_to_create:
        bulk_process(
            model=InventorySnapshot,
            data=inventory_snapshots_to_create,
            unique_fields=['product_id', 'date'],
            update_fields=['stock', 'price_with_tax', 'weighted_average_price']
        )

    return {
        "created_products": list(products_map.values()),
        "created_snapshots": inventory_snapshots_to_create
    }


def process_order_winpharma(pharmacy, data):
    """
    Process WinPharma order data to create or update suppliers, products, and orders in the database.

    Args:
        pharmacy: Pharmacy instance associated with the data.
        data: List of dictionaries representing order data.

    Returns:
        None
    """
    preprocessed_data = []
    for obj in data:
        try:
            # Clean and validate order data
            id_cmd = obj.get('idCmd')
            if isinstance(id_cmd, str):
                internal_id = int(id_cmd.split('-')[0])
            else:
                internal_id = int(id_cmd)

            code_fourn = obj.get('codeFourn')
            if not code_fourn:
                raise ValueError("Missing 'codeFourn'")

            nom_fourn = obj.get('nomFourn', obj.get('code_supplier', ''))

            etape = obj.get('etape', '')
            envoi = parse_date(obj.get('envoi'))
            date_livraison = parse_date(obj.get('dateLivraison'), False)

            produits = obj.get('produits', [])
            if not isinstance(produits, list):
                raise TypeError("'produits' should be a list")

            cleaned_produits = []
            for line in produits:
                try:
                    prod_id = line['prodId']
                    qte = int(line.get('qte', 0))
                    qte_ug = int(line.get('qteUG', 0))
                    qte_a = int(line.get('qteA', 0))
                    qte_r = int(line.get('qteR', 0))
                    qte_ar = int(line.get('qteAReceptionner', 0))
                    qte_ec = int(line.get('qteEC', 0))

                    cleaned_produits.append({
                        'prodId': prod_id,
                        'qte': qte,
                        'qteUG': qte_ug,
                        'qteA': qte_a,
                        'qteR': qte_r,
                        'qteAReceptionner': qte_ar,
                        'qteEC': qte_ec
                    })
                except (ValueError, TypeError, KeyError) as e:
                    logger.warning(f"Error preprocessing product in order {id_cmd}: {e}")
                    continue

            preprocessed_data.append({
                'idCmd': internal_id,
                'codeFourn': code_fourn,
                'nomFourn': nom_fourn,
                'etape': etape,
                'envoi': envoi,
                'dateLivraison': date_livraison,
                'produits': cleaned_produits
            })
        except (ValueError, TypeError) as e:
            logger.warning(f"Error preprocessing order {obj.get('idCmd', 'unknown')}: {e}")
            continue

    # Collect unique supplier codes and prepare supplier data
    supplier_data = []
    seen_suppliers = set()

    for obj in preprocessed_data:
        supplier_key = (pharmacy.id, obj['codeFourn'])
        if supplier_key not in seen_suppliers:
            supplier_data.append({
                'pharmacy_id': pharmacy.id,
                'code_supplier': obj['codeFourn'],
                'name': obj['nomFourn']
            })
            seen_suppliers.add(supplier_key)

    # Create or update suppliers
    suppliers = bulk_process(
        model=Supplier,
        data=supplier_data,
        unique_fields=['pharmacy_id', 'code_supplier'],
        update_fields=['name']
    )

    # Map suppliers by their codes for easy access
    suppliers_map = {supplier.code_supplier: supplier for supplier in suppliers}

    # Extract all product IDs from the order lines
    product_ids = {line['prodId'] for obj in data for line in obj.get('produits', [])}

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

    # Create or update InternalProduct instances
    products = bulk_process(
        model=InternalProduct,
        data=internal_product_data,
        unique_fields=['pharmacy_id', 'internal_id'],
        update_fields=['name']
    )

    # Map InternalProduct instances by their internal_id for easy access
    internal_products_map = {product.internal_id: product for product in products}

    # Prepare order data for bulk processing
    order_data = [
        {
            'pharmacy_id': pharmacy.id,
            'supplier_id': suppliers_map[obj['codeFourn']].id,
            'internal_id': obj['idCmd'],
            'step': obj['etape'],
            'sent_date': obj['envoi'],
            'delivery_date': obj['dateLivraison'],
        }
        for obj in preprocessed_data
    ]

    # Create or update Order instances
    orders = bulk_process(
        model=Order,
        data=order_data,
        unique_fields=['internal_id', 'pharmacy_id'],
        update_fields=['supplier_id', 'step', 'sent_date', 'delivery_date']
    )

    # Map Order instances by their internal_id for easy access
    order_map = {order.internal_id: order for order in orders}

    # Prepare ProductOrder data for bulk processing
    product_order_data = []
    for obj in preprocessed_data:
        internal_id = obj['idCmd']
        for line in obj['produits']:
            product = internal_products_map.get(line['prodId'])
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
    bulk_process(
        model=ProductOrder,
        data=product_order_data,
        unique_fields=['product', 'order'],
        update_fields=['qte', 'qte_r', 'qte_a', 'qte_ug', 'qte_ec', 'qte_ar']
    )


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
            # Clean and validate sales data
            code_13_ref = obj.get('code13Ref', None)

            prod_id = str(obj['prodId'])
            heure = parse_date(obj.get('heure'))
            code_operateur = obj.get('codeOperateur')
            qte = int(obj.get('qte', 0))

            preprocessed_data.append({
                'code13Ref': code_13_ref,
                'prodId': prod_id,
                'heure': heure,
                'codeOperateur': code_operateur,
                'qte': qte
            })
        except (ValueError, TypeError, KeyError) as e:
            logger.warning(f"Error preprocessing sale record {obj.get('prodId', 'unknown')}: {e}")
            continue

    # Collect unique product references
    code_13_refs = {obj['code13Ref'] for obj in preprocessed_data if obj['code13Ref']}

    with transaction.atomic():
        # Retrieve existing GlobalProduct instances
        global_products_map = {
            gp.code_13_ref: gp for gp in GlobalProduct.objects.filter(code_13_ref__in=code_13_refs).only('code_13_ref', 'name')
        }

        # Identify missing references and create them
        missing_refs = code_13_refs - global_products_map.keys()
        if missing_refs:
            GlobalProduct.objects.bulk_create(
                [GlobalProduct(code_13_ref=ref, name='Default Name') for ref in missing_refs]
            )
            # Update the map with newly created GlobalProducts
            new_global_products = GlobalProduct.objects.filter(code_13_ref__in=missing_refs).only('code_13_ref', 'name')
            global_products_map.update({gp.code_13_ref: gp for gp in new_global_products})

    # Prepare a set of product IDs to process
    product_ids = {obj['prodId'] for obj in preprocessed_data}

    # Retrieve existing InternalProduct instances for the pharmacy
    existing_products = InternalProduct.objects.filter(
        pharmacy_id=pharmacy.id,
        internal_id__in=product_ids
    ).only('internal_id', 'name', 'code_13_ref', 'TVA')

    # Prepare data for InternalProduct
    internal_product_data = []
    seen_products = set()  # Set to avoid duplicate products

    # Add existing products to the list (without modification)
    for product in existing_products:
        internal_product_data.append({
            'internal_id': str(product.internal_id),
            'pharmacy_id': pharmacy.id,
            'name': product.name,  # Retain existing name
            'code_13_ref': product.code_13_ref,  # Retain code_13_ref
            'TVA': product.TVA  # Retain TVA
        })
        seen_products.add(str(product.internal_id))

    # Add non-existing products with 'empty' as the name
    for obj in preprocessed_data:
        if obj['prodId'] not in seen_products:
            code_13_ref = obj.get('code13Ref') or None
            global_product_instance = global_products_map.get(code_13_ref)

            internal_product_data.append({
                'pharmacy_id': pharmacy.id,
                'internal_id': obj['prodId'],
                'code_13_ref': global_product_instance,
                'name': obj.get('nom', ''),
                'TVA': float(obj.get('TVA', 0.0)),
            })
            seen_products.add(obj['prodId'])

    # Create or update InternalProduct instances
    try:
        products = bulk_process(
            model=InternalProduct,
            data=internal_product_data,
            unique_fields=['pharmacy_id', 'internal_id'],
            update_fields=['code_13_ref', 'name', 'TVA']
        )
    except Exception as e:
        logger.error(f"Error processing internal products: {e}")
        raise

    # Create a mapping of created/updated InternalProduct instances
    products_map = {str(product.internal_id): product for product in products}

    latest_snapshots = (
        InventorySnapshot.objects
        .filter(product=OuterRef('id'))
        .order_by('-created_at')
        .values('id')[:1])

    internal_products = InternalProduct.objects.filter(pharmacy=pharmacy, internal_id__in=product_ids).annotate(
        latest_snapshot_id=Subquery(latest_snapshots)
    )
    internal_products_map = {str(product.internal_id): product.latest_snapshot_id for product in internal_products}

    sales_data = []
    sales_seen = set()  # Set to avoid duplicate sales

    for obj in preprocessed_data:
        product_id = obj['prodId']
        product = products_map.get(product_id)
        if not product:
            logger.warning(f"Product with internal_id {product_id} not found.")
            continue

        # Retrieve the latest stock snapshot for the product
        snapshot_id = internal_products_map.get(obj['prodId'])

        if not snapshot_id:
            logger.warning(f"No snapshot found for product {product_id}.")
            continue

        # Check if this sale already exists
        sale_key = (snapshot_id, obj['heure'], obj['codeOperateur'])  # Unique key for the sale
        if sale_key in sales_seen:
            continue
        sales_seen.add(sale_key)

        sales_data.append({
            'product_id': snapshot_id,
            'time': str(obj['heure']),
            'operator_code': obj['codeOperateur'],
            'quantity': obj['qte']
        })

    # Process sales in bulk
    try:
        sales = bulk_process(
            model=Sales,
            data=sales_data,
            unique_fields=['time'],
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
        None
    """
    preprocessed_data = []
    for obj in data:
        try:
            # Clean and validate stock data
            produit_id = str(obj['produit_id'])
            libelle_produit = str(obj['libelle_produit']) if obj.get('libelle_produit') else ""
            tva = float(obj['taux_Tva']) if obj.get('taux_Tva') is not None else 0.0
            prix_achat = float(obj['px_achat_PMP_HT']) if obj.get('px_achat_PMP_HT') else 0.0
            stock = int(obj['qte_stock']) if obj.get('qte_stock') else 0
            price_with_tax = Decimal(obj['px_vte_TTC']).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if obj.get(
                'px_vte_TTC') else Decimal('0.00')
            code_13_ref = None
            if isinstance(obj.get('code_produit'), list):
                for code in obj['code_produit']:
                    if code.get('referent'):
                        code_13_ref = code.get('code')
                        break

        except (ValueError, TypeError) as e:
            logger.warning(f"Error preprocessing data for product {obj.get('produit_id', 'unknown')}: {e}")
            continue

        preprocessed_data.append({
            'produit_id': produit_id,
            'libelle_produit': libelle_produit,
            'tva': tva,
            'prix_achat': prix_achat,
            'stock': stock,
            'price_with_tax': price_with_tax,
            'code13Ref': code_13_ref or "",

        })

    # Collect unique GlobalProduct references
    code_13_refs = {obj['code13Ref'] for obj in preprocessed_data if obj['code13Ref']}

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
            'internal_id': obj['produit_id'],
            'name': obj['libelle_produit'],
            'TVA': obj['tva'],
            'code_13_ref': global_products_map.get(obj['code13Ref'], None),

        }
        for obj in preprocessed_data
    ]

    # Create or update InternalProduct instances
    try:
        products = bulk_process(
            model=InternalProduct,
            data=internal_products_data,
            unique_fields=['pharmacy_id', 'internal_id'],
            update_fields=['name', 'TVA']
        )
    except Exception as e:
        logger.error(f"Error during bulk process of products: {e}")
        raise

    # Map created/updated InternalProduct instances by internal_id
    products_map = {str(product.internal_id): product for product in products}

    # Parse the snapshot date
    snapshot_date = parse_date(date_str, is_datetime=False)

    # Optimize queries to get the latest snapshots for each product
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
        produit_id = obj['produit_id']
        product = products_map.get(produit_id)
        if not product:
            logger.warning(f"InternalProduct with internal_id {produit_id} not found.")
            continue

        # Retrieve the latest snapshot for this product
        last_snapshot = latest_snapshots_map.get(produit_id)

        # Determine if a new snapshot is needed
        needs_update = False
        if not last_snapshot:
            needs_update = True
        else:
            if (
                last_snapshot['stock'] != obj['stock'] or
                last_snapshot['price_with_tax'] != obj['price_with_tax'] or
                last_snapshot['weighted_average_price'] != Decimal(obj['prix_achat']).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            ):
                needs_update = True

        if needs_update:
            inventory_snapshots_data.append({
                'product_id': product.id,
                'stock': obj['stock'],
                'price_with_tax': obj['price_with_tax'],
                'weighted_average_price': Decimal(obj['prix_achat']).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP),
                'date': snapshot_date
            })

    # Create or update InventorySnapshot instances
    try:
        bulk_process(
            model=InventorySnapshot,
            data=inventory_snapshots_data,
            unique_fields=['product_id', 'date'],
            update_fields=['stock', 'price_with_tax', 'weighted_average_price']
        )
    except Exception as e:
        logger.error(f"Error during bulk process of inventory snapshots: {e}")
        raise

    return {
        "created_products": list(products_map.values()),
        "created_snapshots": inventory_snapshots_data
    }


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
            commande_id = str(obj['commande_id'])
            id_fournisseur = str(obj['id_fournisseur'])
            libelle_fournisseur = str(obj.get('libelle_fournisseur', obj.get('id_fournisseur', '')))
            etat_commande = str(obj.get('etat_commande', ''))
            date_transmission = parse_date(obj.get('date_transmission'))
            date_reception = parse_date(obj.get('date_reception'), is_datetime=False)

            lignes = obj.get('lignes', [])
            if not isinstance(lignes, list):
                raise TypeError("'lignes' should be a list")

            cleaned_lignes = []
            for line in lignes:
                try:
                    produit_id = str(line['produit_id'])
                    qte_cde = int(line.get('qte_cde', 0))
                    total_recu = int(line.get('total_recu', 0))
                    total_ug_liv = int(line.get('total_ug_liv', 0))

                    cleaned_lignes.append({
                        'produit_id': produit_id,
                        'qte_cde': qte_cde,
                        'total_recu': total_recu,
                        'total_ug_liv': total_ug_liv
                    })
                except (ValueError, TypeError, KeyError) as e:
                    logger.warning(f"Error preprocessing product line in order {commande_id}: {e}")
                    continue

            preprocessed_data.append({
                'commande_id': commande_id,
                'id_fournisseur': id_fournisseur,
                'libelle_fournisseur': libelle_fournisseur,
                'etat_commande': etat_commande,
                'date_transmission': date_transmission,
                'date_reception': date_reception,
                'lignes': cleaned_lignes
            })
        except (ValueError, TypeError, KeyError) as e:
            logger.warning(f"Error preprocessing order {obj.get('commande_id', 'unknown')}: {e}")
            continue

    # Step 1: Collect unique supplier codes and prepare supplier data
    supplier_data = []
    seen_suppliers = set()

    for obj in preprocessed_data:
        supplier_key = (pharmacy.id, obj['id_fournisseur'])
        if supplier_key not in seen_suppliers:
            supplier_data.append({
                'pharmacy_id': pharmacy.id,
                'code_supplier': obj['id_fournisseur'],
                'name': obj['libelle_fournisseur']
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
    product_ids = {line['produit_id'] for obj in preprocessed_data for line in obj['lignes']}

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

    internal_products_map = {product.internal_id: product for product in products}

    # Step 2: Prepare order data
    order_data = []
    for obj in preprocessed_data:
        try:
            order_data.append({
                'internal_id': obj['commande_id'],
                'pharmacy_id': pharmacy.id,
                'supplier_id': suppliers_map[obj['id_fournisseur']].id,
                'step': obj['etat_commande'],
                'sent_date': obj['date_transmission'],
                'delivery_date': obj['date_reception'],
            })
        except KeyError as e:
            logger.warning(f"Missing key in order {obj['commande_id']}: {e}")
            continue
        except Exception as e:
            logger.error(f"Error processing order {obj['commande_id']}: {e}")
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
    order_map = {order.internal_id: order for order in orders}

    # Step 3: Prepare product-order association data
    product_order_data = []
    existing_associations = set()  # Set to keep track of existing associations

    for obj in preprocessed_data:
        internal_id = obj['commande_id']
        order = order_map.get(internal_id)
        if not order:
            logger.warning(f"Order with internal_id {internal_id} not found.")
            continue

        for line in obj['lignes']:
            product_id = line['produit_id']
            product = internal_products_map.get(product_id)
            if not product:
                logger.warning(f"InternalProduct with internal_id {product_id} not found.")
                continue

            # Ensure quantities are integers
            qte = line.get('qte_cde', 0)
            qte_a = line.get('qte_cde', 0)
            qte_ug = line.get('total_ug_liv', 0)
            qte_r = line.get('total_recu', 0)
            qte_ec = line.get('qte_cde', 0) - line.get('total_recu', 0)
            qte_ar = line.get('qte_cde', 0) - line.get('total_recu', 0)

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

    # Step 4: Create or update ProductOrder instances
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

    return {
        "created_suppliers": [supplier for supplier in suppliers if supplier.pk is not None],
        "created_products": [product for product in products if product.pk is not None],
        "created_orders": [order for order in orders if order.pk is not None],
        "created_product_orders": product_order_data
    }


def process_vente_dexter(pharmacy, data):
    """
    Creates or updates sales and their associated product lines using bulk_process.

    Args:
        pharmacy: Pharmacy instance to associate with the data.
        data: List of dictionaries representing Dexter sales data.
        snapshot_date_str (str): Date string representing the snapshot date.

    Returns:
        dict: A dictionary containing lists of created products, sales, and product-order associations.
    """
    preprocessed_data = []
    for obj in data:
        try:
            # Extract and validate sales data
            for invoice in obj.get('factures', []):
                for line in invoice.get('lignes_de_facture', []):
                    produit_id = line.get('produit_id')
                    if not produit_id:
                        logger.warning(f"Missing 'produit_id' in invoice line: {line}")
                        continue  # Skip lines without produit_id

                    preprocessed_data.append({
                        'produit_id': str(produit_id),
                        'libelle_produit': str(line.get('libelle_produit', 'Unknown')),
                        'quantite': int(line.get('quantite', 0)),
                        'date_acte': obj.get('date_acte'),
                        'code_operateur': str(obj.get('code_operateur', ''))
                    })
        except (ValueError, TypeError, KeyError) as e:
            logger.warning(f"Error preprocessing sales data for object {obj.get('commande_id', 'unknown')}: {e}")
            continue

    # Step 1: Collect unique product IDs and prepare InternalProduct data
    internal_product_data = []
    seen_products = set()  # Used to avoid duplicates
    for record in preprocessed_data:
        produit_id = record['produit_id']
        unique_key = (produit_id, pharmacy.id)
        if unique_key not in seen_products:
            seen_products.add(unique_key)
            internal_product_data.append({
                'internal_id': produit_id,
                'pharmacy_id': pharmacy.id,
                'name': record['libelle_produit']
            })

    # Create or update InternalProduct instances
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

    # Create a mapping of InternalProduct instances for quick lookup
    product_ids = [product.id for product in products]

    # Step 2: Retrieve the latest InventorySnapshot for each product
    latest_snapshots = (
        InventorySnapshot.objects
        .filter(product=OuterRef('id'))
        .order_by('-created_at')
        .values('id')[:1])

    internal_products = InternalProduct.objects.filter(pharmacy=pharmacy, id__in=product_ids).annotate(
        latest_snapshot_id=Subquery(latest_snapshots)
    )
    # Créer un mapping des produits internes pour les associer rapidement aux lignes de commande
    internal_products_map = {str(product.internal_id): product.latest_snapshot_id for product in internal_products}

    sales_data = []
    sales_seen = set()  # Set to keep track of existing sales to avoid duplicates

    for record in preprocessed_data:
        produit_id = record['produit_id']
        snapshot_id = internal_products_map.get(produit_id)

        if not snapshot_id:
            logger.warning(f"No snapshot found for product {produit_id}. Skipping sale record.")
            continue  # Skip if there's no snapshot associated

        # Create a unique key for the sale to prevent duplicates
        sale_key = (snapshot_id, record['date_acte'], record['code_operateur'])
        if sale_key in sales_seen:
            continue  # Skip if sale has already been processed

        sales_seen.add(sale_key)

        # Ensure quantity is a valid integer
        try:
            quantity = int(record['quantite'])
        except ValueError:
            logger.warning(f"Invalid quantity '{record['quantite']}' for product {produit_id}. Setting quantity to 0.")
            quantity = 0

        # Append the sale record
        sales_data.append({
            'product_id': snapshot_id,
            'quantity': quantity,
            'time': parse_date(record['date_acte']),
            'operator_code': record['code_operateur']
        })

    # Step 4: Create or update Sales instances
    try:
        bulk_process(
            model=Sales,
            data=sales_data,
            unique_fields=['product_id', 'time', 'operator_code'],
            update_fields=['quantity']
        )
    except Exception as e:
        logger.error(f"Error processing sales: {e}")
        raise

    return {
        "created_products": [product for product in products if product.pk is not None],
        "created_sales": sales_data
    }
