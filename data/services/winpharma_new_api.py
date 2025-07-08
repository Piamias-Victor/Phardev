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
    Process WinPharma NEW API product data to create or update global products, internal products, and inventory snapshots.
    
    Expected data format from new API:
    [{"cip_pharma": "062044623", "produits": [{"ProdId": 123, "Nom": "...", "Code13Ref": "...", "Stock": 5, "PrixTTC": 10.50, "PrixMP": 9.20}]}]

    Args:
        pharmacy: Pharmacy instance associated with the data.
        data: Response from new API - list with pharmacy data wrapper.

    Returns:
        A dictionary containing lists of created products and snapshots.
    """
    # Extract products from the new API wrapper structure
    if not data or not isinstance(data, list) or len(data) == 0:
        logger.warning("No data or invalid data structure received")
        return {"products": [], "snapshots": []}
    
    pharmacy_data = data[0]  # First element contains the pharmacy data
    products_raw = pharmacy_data.get('produits', [])
    
    logger.info(f"Processing {len(products_raw)} products for pharmacy {pharmacy.id_nat}")
    
    preprocessed_data = []
    for obj in products_raw:
        try:
            # Map new API fields to our internal structure
            prod_id = obj.get('ProdId')
            if not prod_id or int(prod_id) < 0:
                continue
                
            # Note: No TVA field in new API, will be set to None
            preprocessed_data.append({
                'product_id': str(prod_id),
                'name': obj.get('Nom', ''),
                'code_13_ref': obj.get('Code13Ref') or None,
                'TVA': None,  # Not available in new API
                'stock': common.clamp(int(obj.get('Stock', 0)), -32768, 32767),
                'price_with_tax': min(
                    Decimal(str(obj.get('PrixTTC', 0))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP),
                    Decimal('99999999.99')
                ),
                'weighted_average_price': min(
                    Decimal(str(obj.get('PrixMP', 0))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP),
                    Decimal('99999999.99')
                ),
            })

        except (ValueError, TypeError) as e:
            logger.warning(f"Error preprocessing data for product {obj.get('ProdId', 'unknown')}: {e}")
            continue

    if not preprocessed_data:
        logger.info("No valid products to process")
        return {"products": [], "snapshots": []}

    # Rest of the logic remains the same as original
    # Collect unique GlobalProduct references
    code_13_refs = {obj['code_13_ref'] for obj in preprocessed_data if obj['code_13_ref']}

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
            'internal_id': obj['product_id'],
            'code_13_ref': global_products_map.get(obj['code_13_ref'], None),
            'name': obj['name'],
            'TVA': obj['TVA'],  # Will be None for new API
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

        if not product:
            continue

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

    logger.info(f"Successfully processed {len(products)} products, {len(inventory_snapshots_data)} snapshots")
    return {"products": products, "snapshots": inventory_snapshots_data}


def process_order(pharmacy, data):
    """
    Process WinPharma NEW API order data to create or update suppliers, products, and orders in the database.
    
    Expected data format from new API:
    [{"cip_pharma": "062044623", "achats": [{"id": 123, "codeFourn": "3", "nomFourn": "CERP", "dateLivraison": "2025-05-20T00:00:00", 
      "dateEnvoi": "2021-10-25T00:00:00", "channel": "pml", "lignes": [{"prodId": 123, "qteC": 2, "qteR": 2, "qteUG": 0, "qteEC": 0}]}]}]

    Args:
        pharmacy: Pharmacy instance associated with the data.
        data: Response from new API - list with pharmacy data wrapper.

    Returns:
        dict: A dictionary containing lists of created suppliers, products, orders, and product-order associations.
    """
    # Extract orders from the new API wrapper structure
    if not data or not isinstance(data, list) or len(data) == 0:
        logger.warning("No data or invalid data structure received")
        return {"suppliers": [], "products": [], "orders": [], "product_orders": []}
    
    pharmacy_data = data[0]  # First element contains the pharmacy data
    orders_raw = pharmacy_data.get('achats', [])
    
    logger.info(f"Processing {len(orders_raw)} orders for pharmacy {pharmacy.id_nat}")
    
    preprocessed_data = []
    for obj in orders_raw:
        try:
            # Map new API fields to our internal structure
            order_id = obj.get('id')
            if not order_id:
                continue
                
            supplier_id = obj.get('codeFourn')
            if not supplier_id:
                raise ValueError("Missing 'codeFourn'")
                
            supplier_name = obj.get('nomFourn', supplier_id)
            
            # Map channel to step (semantic change + convert to integer)
            channel = obj.get('channel', '')
            # Convertir le channel string en step integer pour compatibilité DB
            channel_to_step = {
                'pml': 1,
                'email': 2, 
                'autre': 3,
                '': 0
            }
            step = channel_to_step.get(channel.lower(), 0)
            
            sent_date = common.parse_date(obj.get('dateEnvoi'))
            delivery_date = common.parse_date(obj.get('dateLivraison'), False)

            products = []
            for line in obj.get('lignes', []):
                try:
                    prod_id = line.get('prodId')
                    if not prod_id or int(prod_id) < 0:
                        continue

                    product_id = str(prod_id)
                    qte_c = common.clamp(int(line.get('qteC', 0)), -32768, 32767)  # Commanded quantity
                    qte_r = common.clamp(int(line.get('qteR', 0)), -32768, 32767)  # Received quantity
                    qte_ug = common.clamp(int(line.get('qteUG', 0)), -32768, 32767)  # Free units
                    qte_ec = common.clamp(int(line.get('qteEC', 0)), -32768, 32767)  # Exchange quantity
                    
                    # Calculate missing fields based on available data
                    qte_a = qte_c  # Expected quantity = commanded quantity
                    qte_ar = max(0, qte_c - qte_r)  # Quantity to receive = commanded - received

                    products.append({
                        'product_id': product_id,
                        'qte': qte_c,  # Commanded quantity
                        'qte_r': qte_r,  # Received quantity
                        'qte_a': qte_a,  # Expected quantity (calculated)
                        'qte_ug': qte_ug,  # Free units
                        'qte_ec': qte_ec,  # Exchange quantity
                        'qte_ar': qte_ar,  # Quantity to receive (calculated)
                        # New fields available in new API (could be used later)
                        'prix': line.get('prix', 0),  # Initial price
                        'remise_final': line.get('remiseFinal', 0),  # Final discount
                    })
                except (ValueError, TypeError, KeyError) as e:
                    logger.warning(f"Error preprocessing product in order {order_id}: {e}")
                    continue

            preprocessed_data.append({
                'order_id': order_id,
                'supplier_id': supplier_id,
                'supplier_name': supplier_name,
                'step': step,  # Channel mapped to step
                'sent_date': sent_date,
                'delivery_date': delivery_date,
                'products': products
            })
        except (ValueError, TypeError) as e:
            logger.warning(f"Error preprocessing order {obj.get('id', 'unknown')}: {e}")
            continue

    if not preprocessed_data:
        logger.info("No valid orders to process")
        return {"suppliers": [], "products": [], "orders": [], "product_orders": []}

    # Rest of the logic remains the same as original
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
    internal_products_map = {str(product.internal_id): product for product in products}

    # Prepare order data for bulk processing
    order_data = [
        {
            'internal_id': obj['order_id'],
            'pharmacy_id': pharmacy.id,
            'supplier_id': suppliers_map[obj['supplier_id']].id,
            'step': obj['step'],  # Now contains channel value
            'sent_date': obj['sent_date'],
            'delivery_date': obj['delivery_date'],
        }
        for obj in preprocessed_data if obj['supplier_id'] in suppliers_map
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
        order = order_map.get(internal_id)
        
        if not order:
            continue
            
        for line in obj['products']:
            product = internal_products_map.get(line['product_id'])
            if not product:
                continue

            product_order_data.append({
                'product': product,
                'order': order,
                'qte': line['qte'],
                'qte_r': line['qte_r'],
                'qte_a': line['qte_a'],
                'qte_ug': line['qte_ug'],
                'qte_ec': line['qte_ec'],
                'qte_ar': line['qte_ar']
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

    logger.info(f"Successfully processed {len(suppliers)} suppliers, {len(products)} products, {len(orders)} orders")
    return {
        "suppliers": suppliers,
        "products": products,
        "orders": orders,
        "product_orders": product_order_data,
    }


def process_sales(pharmacy, data):
    """
    Process sales records for a pharmacy and its products from NEW API.
    
    Expected data format from new API:
    [{"cip_pharma": "062044623", "ventes": [{"id": 123, "heure": "2025-05-20T08:32:29", 
      "lignes": [{"prodId": 123, "qte": 1}]}]}]

    Args:
        pharmacy: Pharmacy instance associated with the data.
        data: Response from new API - list with pharmacy data wrapper.

    Returns:
        None
    """
    # Extract sales from the new API wrapper structure
    if not data or not isinstance(data, list) or len(data) == 0:
        logger.warning("No data or invalid data structure received")
        return
    
    pharmacy_data = data[0]  # First element contains the pharmacy data
    sales_raw = pharmacy_data.get('ventes', [])
    
    logger.info(f"Processing {len(sales_raw)} sales records for pharmacy {pharmacy.id_nat}")
    
    preprocessed_data = []
    for sale in sales_raw:
        try:
            sale_time = sale.get('heure')
            for line in sale.get('lignes', []):
                prod_id = line.get('prodId')
                if not prod_id or int(prod_id) < 0:
                    continue
                    
                preprocessed_data.append({
                    'product_id': str(prod_id),
                    'date': sale_time,  # Date from sale header
                    'qte': int(line.get('qte', 0))
                })
        except (ValueError, TypeError, KeyError) as e:
            logger.warning(f"Error preprocessing sale record {sale.get('id', 'unknown')}: {e}")
            continue

    if not preprocessed_data:
        logger.info("No valid sales to process")
        return

    # Rest of the logic remains the same as original
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
        day = common.parse_date(obj['date'], False)
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
            
        sales_data.append({
            'product_id': snapshot_id,
            'quantity': common.clamp(total_qte, -32768, 32767),
            'date': day,
        })

    # Process sales in bulk
    if sales_data:
        try:
            common.bulk_process(
                model=Sales,
                data=sales_data,
                unique_fields=['product_id', 'date'],
                update_fields=['quantity']
            )
            logger.info(f"Successfully processed {len(sales_data)} sales records")
        except Exception as e:
            logger.error(f"Error processing sales: {e}")
            raise
    else:
        logger.info("No sales data to process")