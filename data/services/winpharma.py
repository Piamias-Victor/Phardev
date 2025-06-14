from datetime import date
from decimal import Decimal, ROUND_HALF_UP
import logging

from django.db import transaction
from django.db.models import OuterRef, Subquery, F, Window
from django.db.models.functions import RowNumber

from data.models import GlobalProduct, InternalProduct, ProductOrder, Supplier, Order, Sales, InventorySnapshot
from data.services import common
logger = logging.getLogger(__name__)


def process_product(pharmacy, data):
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
                'stock': common.clamp(int(obj.get('stock', 0)), -32768, 32767),
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
        products = common.bulk_process(
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
        common.bulk_process(
            model=InventorySnapshot,
            data=inventory_snapshots_data,
            unique_fields=['product_id', 'date'],
            update_fields=['stock', 'price_with_tax', 'weighted_average_price']
        )


def process_order(pharmacy, data):
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
            sent_date = common.parse_date(obj.get('envoi'))
            delivery_date = common.parse_date(obj.get('dateLivraison'), False)

            products = []
            for line in obj.get('produits', []):
                try:
                    if int(line.get('prodId')) < 0:
                        continue

                    product_id = str(line['prodId'])
                    qte = common.clamp(int(line.get('qte', 0)), -32768, 32767)  # SmallIntegerField
                    qte_r = common.clamp(int(line.get('qteR', 0)), -32768, 32767)  # SmallIntegerField
                    qte_a = common.clamp(int(line.get('qteA', 0)), -32768, 32767)  # SmallIntegerField
                    qte_ug = common.clamp(int(line.get('qteUG', 0)), -32768, 32767)  # SmallIntegerField
                    qte_ec = common.clamp(int(line.get('qteEC', 0)), -32768, 32767)  # SmallIntegerField
                    qte_ar = common.clamp(int(line.get('qteAReceptionner', 0)), -32768, 32767)  # SmallIntegerField

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
        suppliers = common.bulk_process(
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
        products = common.bulk_process(
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
        orders = common.bulk_process(
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
        common.bulk_process(
            model=ProductOrder,
            data=product_order_data,
            unique_fields=['order', 'product'],
            update_fields=['qte', 'qte_r', 'qte_a', 'qte_ug', 'qte_ec', 'qte_ar']
        )
    except Exception as e:
        logger.error(f"Error processing product-order associations: {e}")
        raise


def process_sales(pharmacy, data):
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
            'quantity': common.clamp(int(value[1]), -32768, 32767),
            'date': common.parse_date(value[0], False),
        })

    # Process sales in bulk
    try:
        common.bulk_process(
            model=Sales,
            data=sales_data,
            unique_fields=['product_id', 'date'],
            update_fields=['quantity']
        )
    except Exception as e:
        logger.error(f"Error processing sales: {e}")
        raise