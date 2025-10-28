from decimal import Decimal, ROUND_HALF_UP
import logging
import traceback
from datetime import date

from django.db import transaction
from django.db.models import OuterRef, Subquery, F, Window
from django.db.models.functions import RowNumber


from data.models import GlobalProduct, InternalProduct, ProductOrder, Supplier, Order, Sales, InventorySnapshot
from data.services import common

logger = logging.getLogger(__name__)


def process_stock(pharmacy, data, date_str):
    """
    Creates or updates products and their inventory snapshots using common.bulk_process.

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
                'stock': common.clamp(int(obj['qte_stock']) if obj.get('qte_stock') else 0, -32768, 32767),
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
        products = common.bulk_process(
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
    snapshot_date = common.parse_date(date_str, is_datetime=False)

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
        common.bulk_process(
            model=InventorySnapshot,
            data=inventory_snapshots_data,
            unique_fields=['product_id', 'date'],
            update_fields=['stock', 'price_with_tax', 'weighted_average_price']
        )


def process_achat(pharmacy, data):
    """
    Creates or updates orders and their associated product lines using common.bulk_process.

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
            sent_date = common.parse_date(obj.get('date_transmission'))
            delivery_date = common.parse_date(obj.get('date_reception'), is_datetime=False)

            products = []
            for line in obj.get('lignes', []):
                try:
                    if int(line.get('produit_id')) < 0:
                        continue

                    product_id = str(line['produit_id'])
                    qte_cde = int(line.get('qte_cde', 0))
                    total_recu = int(line.get('total_recu', 0))
                    total_ug_liv = int(line.get('total_ug_liv', 0))
                    px_achat_pmp_ht = line.get('px_achat_PMP_HT')

                    products.append({
                        'product_id': product_id,
                        'qte_cde': qte_cde,
                        'total_recu': total_recu,
                        'total_ug_liv': total_ug_liv,
                        'px_achat_PMP_HT': px_achat_pmp_ht
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

    # Prepare a set of supplier IDs
    supplier_ids = {obj['supplier_id'] for obj in preprocessed_data}

    # Prepare data for bulk processing of suppliers
    supplier_data = []
    for supplier_id in supplier_ids:
        # Find the first occurrence with supplier name
        supplier_name = next(
            (obj['supplier_name'] for obj in preprocessed_data if obj['supplier_id'] == supplier_id), ''
        )
        supplier_data.append({
            'internal_id': supplier_id,
            'name': supplier_name
        })

    # Create or update suppliers
    try:
        suppliers = common.bulk_process(
            model=Supplier,
            data=supplier_data,
            unique_fields=['internal_id'],
            update_fields=['name']
        )
    except Exception as e:
        logger.error(f"Error processing suppliers: {e}")
        raise

    # Map suppliers for quick lookup
    suppliers_map = {supplier.internal_id: supplier for supplier in suppliers}

    # Prepare a set of product IDs
    product_ids = set()
    for obj in preprocessed_data:
        for line in obj['products']:
            product_ids.add(line['product_id'])

    # Query existing InternalProduct instances to get their names
    existing_products = InternalProduct.objects.filter(
        pharmacy=pharmacy,
        internal_id__in=product_ids
    ).only('internal_id', 'pharmacy_id', 'name')

    # Prepare data for internal products, retaining existing names
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
        orders = common.bulk_process(
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
            qte = common.clamp(int(line.get('qte_cde', 0)), -32768, 32767)  # SmallIntegerField
            qte_a = common.clamp(int(line.get('qte_cde', 0)), -32768, 32767)  # SmallIntegerField
            qte_ug = common.clamp(int(line.get('total_ug_liv', 0)), -32768, 32767)  # SmallIntegerField
            qte_r = common.clamp(int(line.get('total_recu', 0)), -32768, 32767)  # SmallIntegerField

            # Calcul des valeurs et common.clamping
            qte_ec = common.clamp(qte - qte_r, -32768, 32767)  # Écart entre commandé et reçu
            qte_ar = common.clamp(qte - qte_r, -32768, 32767)  # Quantité à réceptionner

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
        common.bulk_process(
            model=ProductOrder,
            data=product_order_data,
            unique_fields=['order', 'product'],
            update_fields=['qte', 'qte_r', 'qte_a', 'qte_ug', 'qte_ec', 'qte_ar']
        )
    except Exception as e:
        logger.error(f"Error processing product-order associations: {e}")
        raise

    # ✅ NOUVEAU : Créer snapshots avec PMP pour produits sans snapshot existant
    snapshot_updates = []
    product_ids_for_snapshot = []
    
    # Collecter les produits avec PMP disponible
    for obj in preprocessed_data:
        order = order_map.get(obj['order_id'])
        if not order or not order.delivery_date:
            continue
            
        for line in obj['products']:
            product_id = line['product_id']
            product = internal_products_map.get(product_id)
            if not product:
                continue
            
            pmp_value = line.get('px_achat_PMP_HT')
            if pmp_value is None or pmp_value <= 0:
                continue
            
            product_ids_for_snapshot.append({
                'product': product,
                'pmp': pmp_value,
                'delivery_date': order.delivery_date
            })
    
    if product_ids_for_snapshot:
        # Récupérer les produits qui n'ont PAS de snapshot
        products_with_ids = [p['product'].id for p in product_ids_for_snapshot]
        products_without_snapshot = set(
            InternalProduct.objects.filter(
                id__in=products_with_ids
            ).exclude(
                id__in=InventorySnapshot.objects.filter(
                    product_id__in=products_with_ids
                ).values_list('product_id', flat=True)
            ).values_list('id', flat=True)
        )
        
        # Créer snapshots UNIQUEMENT pour produits sans snapshot
        for item in product_ids_for_snapshot:
            if item['product'].id in products_without_snapshot:
                snapshot_updates.append({
                    'product_id': item['product'].id,
                    'stock': 0,
                    'price_with_tax': Decimal('0.00'),
                    'weighted_average_price': Decimal(str(item['pmp'])).quantize(
                        Decimal('0.01'), 
                        rounding=ROUND_HALF_UP
                    ),
                    'date': date.today()
                })
        
        # Bulk create snapshots
        if snapshot_updates:
            try:
                common.bulk_process(
                    model=InventorySnapshot,
                    data=snapshot_updates,
                    unique_fields=['product_id', 'date'],
                    update_fields=['weighted_average_price']
                )
                logger.info(f"Created {len(snapshot_updates)} snapshots with PMP from Achat")
            except Exception as e:
                logger.error(f"Error creating snapshots from Achat: {e}")


def process_vente(pharmacy, data):
    """
    Creates or updates sales and their associated product lines using common.bulk_process.

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

                    quantite = int(line.get('quantite', 0))
                    total_net_ttc = line.get('total_net_ttc')
                    
                    # Calculer le prix unitaire TTC
                    unit_price = None
                    if quantite > 0 and total_net_ttc is not None and total_net_ttc > 0:
                        unit_price = Decimal(str(total_net_ttc)) / quantite

                    preprocessed_data.append({
                        'product_id': str(product_id),
                        'qte': quantite,
                        'date': obj.get('date_acte'),
                        'unit_price_ttc': unit_price
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

    # Agréger par (produit, date) avec calcul du prix moyen pondéré
    aggregated_sales = {}
    for obj in preprocessed_data:
        product_id = obj['product_id']
        day = common.parse_date(obj['date'], is_datetime=False)
        key = (product_id, day)
        
        if key not in aggregated_sales:
            aggregated_sales[key] = {
                'qte': obj['qte'],
                'total_montant': (obj['unit_price_ttc'] * obj['qte']) if obj['unit_price_ttc'] else Decimal('0'),
                'has_price': obj['unit_price_ttc'] is not None
            }
        else:
            aggregated_sales[key]['qte'] += obj['qte']
            if obj['unit_price_ttc']:
                aggregated_sales[key]['total_montant'] += obj['unit_price_ttc'] * obj['qte']
                aggregated_sales[key]['has_price'] = True

    sales_data = []
    for (product_id, day), agg in aggregated_sales.items():
        snapshot_id = internal_products_map.get(product_id)
        if not snapshot_id:
            continue
        
        # Calculer le prix unitaire moyen
        unit_price_ttc = None
        if agg['has_price'] and agg['qte'] > 0:
            unit_price_ttc = (agg['total_montant'] / agg['qte']).quantize(
                Decimal('0.01'), 
                rounding=ROUND_HALF_UP
            )
        
        if agg['qte'] > 10000:
            logger.warning(f"Quantité élevée le {day} pour produit {product_id}: {agg['qte']}")
        
        sales_data.append({
            'product_id': snapshot_id,
            'quantity': common.clamp(agg['qte'], -32768, 32767),
            'date': day,
            'unit_price_ttc': float(unit_price_ttc) if unit_price_ttc else None
        })

    # Process sales in bulk
    try:
        common.bulk_process(
            model=Sales,
            data=sales_data,
            unique_fields=['product_id', 'date'],
            update_fields=['quantity', 'unit_price_ttc']
        )
    except Exception as e:
        logger.error(f"Error processing sales: {e}")
        raise