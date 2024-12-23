import traceback
from datetime import datetime, date
from decimal import Decimal, ROUND_HALF_UP
from uuid import UUID
import logging
from decimal import Decimal, ROUND_HALF_UP
from django.db.models import OuterRef, Subquery, Max, F
from datetime import datetime
from django.db import transaction
from django.db.models import OuterRef, Subquery

from data.models import GlobalProduct, InternalProduct, ProductOrder, InventorySnapshot, Supplier, Order, Sales, InventorySnapshot
logger = logging.getLogger(__name__)


def chunked_iterable(iterable, chunk_size):
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i:i + chunk_size]


def parse_date(date_str, is_datetime=True):
    """
    Converts a date string into a datetime object or returns None if the string is empty or poorly formatted.
    """
    if not date_str:
        return None

    formats = ["%Y-%m-%dT%H:%M:%S.%f",  # ISO 8601 with microseconds
               "%Y-%m-%dT%H:%M:%S",  # ISO 8601 without microseconds
               "%Y-%m-%d %H:%M:%S.%f",  # Space separator with microseconds
               "%Y-%m-%d %H:%M:%S",  # Space separator without microseconds
               "%Y-%m-%d"  # Simplified date format
               ]

    for fmt in formats:
        try:
            if is_datetime:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d %H:%M:%S.%f")
            else:
                return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue  # Try the next format

    # If all formats fail, return None
    return None


def bulk_process(model, data, unique_fields, update_fields, chunk_size=1000):
    """
    Processes a batch of objects to create or update them in the database efficiently.

    :param model: The Django model (e.g., MyModel)
    :param data: List of dictionaries representing the objects to be processed
    :param unique_fields: List of fields that uniquely identify an object
    :param update_fields: List of fields to update if the object already exists
    :param chunk_size: Size of the chunks to process at a time
    :return: A combined list of created, updated, and unchanged objects
    """

    # Prepare filters to find existing objects in the database
    filters = {}
    for field in unique_fields:
        field_values = [item[field] for item in data]
        filters[f"{field}__in"] = field_values

    def normalize_value(value):
        """Normalize values for consistent comparisons."""
        if isinstance(value, UUID):
            return str(value)
        elif isinstance(value, Decimal):
            return round(value, 2)
        return str(value)  # Convertir tous les autres types en string

    # Fetch all existing objects based on the filters
    existing_objects = model.objects.filter(**filters)

    # Map existing objects using their unique key
    existing_objects_map = {tuple(normalize_value(getattr(obj, field)) for field in unique_fields): obj for obj in
                            existing_objects}

    # Separate objects into categories: to update, to create, or unchanged
    objects_to_update = []
    objects_to_create = []
    objects_unchanged = []

    for item in data:
        # Generate a lookup key for the current item
        lookup_key = tuple(normalize_value(item.get(field)) for field in unique_fields)

        if lookup_key in existing_objects_map:
            # Existing object: check for changes and update if needed
            existing_obj = existing_objects_map[lookup_key]
            update_needed = False

            for field in update_fields:
                current_value = getattr(existing_obj, field)
                new_value = item[field]

                if isinstance(current_value, Decimal):
                    # Compare decimals with precision rounding
                    current_value = current_value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    new_value = Decimal(new_value).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

                if current_value != new_value:
                    setattr(existing_obj, field, new_value)
                    update_needed = True

            if update_needed:
                objects_to_update.append(existing_obj)
            else:
                objects_unchanged.append(existing_obj)
        else:
            # New object: create an instance
            objects_to_create.append(model(**item))

    all_objects = []  # Combined list of all processed objects

    # Bulk create new objects in chunks
    for chunk in chunked_iterable(objects_to_create, chunk_size):
        with transaction.atomic():
            created_objects_chunk = model.objects.bulk_create(chunk)
            all_objects.extend(created_objects_chunk)

    # Bulk update existing objects in chunks
    for chunk in chunked_iterable(objects_to_update, chunk_size):
        with transaction.atomic():
            model.objects.bulk_update(chunk, fields=update_fields)
            all_objects.extend(chunk)

    # Add unchanged objects to the final list
    all_objects.extend(objects_unchanged)

    # Ensure data integrity: the number of processed objects should match input data
    if len(all_objects) != len(data):
        raise ValueError("Mismatch between input data and processed objects count.")

    print(f"Created: {len(objects_to_create)}, Updated: {len(objects_to_update)}, Unchanged: {len(objects_unchanged)}")

    return all_objects


def process_product_winpharma(pharmacy, data):
    """
    Process WinPharma product data to create or update global products, internal products, and inventory snapshots.

    :param pharmacy: Pharmacy instance associated with the data
    :param data: List of dictionaries representing product data
    """

    # Collect unique `GlobalProduct` references
    code_13_refs = {obj.get('code13Ref') for obj in data if obj.get('code13Ref')}

    # Retrieve or create missing `GlobalProduct` entries
    with transaction.atomic():
        global_products_map = {gp.code_13_ref: gp for gp in GlobalProduct.objects.filter(code_13_ref__in=code_13_refs)}

        missing_refs = code_13_refs - global_products_map.keys()
        if missing_refs:
            GlobalProduct.objects.bulk_create(
                [GlobalProduct(code_13_ref=ref, name='Default Name') for ref in missing_refs])
            global_products_map.update(
                {gp.code_13_ref: gp for gp in GlobalProduct.objects.filter(code_13_ref__in=missing_refs)})

    # Prepare data for `InternalProduct`
    internal_products_data = []
    for obj in data:
        code_13_ref = obj.get('code13Ref') or None
        global_product_instance = global_products_map.get(code_13_ref)

        internal_products_data.append(
            {'pharmacy_id': pharmacy.id, 'internal_id': obj['id'], 'code_13_ref': global_product_instance,
             'name': obj.get('nom', ''), 'TVA': obj.get('TVA', 0.0), })

    # Create or update `InternalProduct`
    products = bulk_process(model=InternalProduct, data=internal_products_data,
                            unique_fields=['pharmacy_id', 'internal_id'], update_fields=['code_13_ref', 'name', 'TVA'])

    # Create a mapping of `InternalProduct` by their IDs
    products_map = {product.internal_id: product for product in products}

    # Retrieve the latest `InventorySnapshot` for each product
    latest_snapshots = InventorySnapshot.objects.filter(product_id__in=[product.id for product in products]).values(
        'product_id', 'product__internal_id').annotate(latest_date=Subquery(
        InventorySnapshot.objects.filter(product_id=OuterRef('product_id')).order_by('-date').values('date')[:1]),
        latest_stock=Subquery(
            InventorySnapshot.objects.filter(product_id=OuterRef('product_id')).order_by('-date').values('stock')[:1]),
        latest_price_with_tax=Subquery(
            InventorySnapshot.objects.filter(product_id=OuterRef('product_id')).order_by('-date').values(
                'price_with_tax')[:1]), latest_weighted_average_price=Subquery(
            InventorySnapshot.objects.filter(product_id=OuterRef('product_id')).order_by('-date').values(
                'weighted_average_price')[:1]))

    # Map latest snapshots by `InternalProduct` ID
    latest_snapshots_map = {snapshot['product__internal_id']: snapshot for snapshot in latest_snapshots}

    # Prepare data for `InventorySnapshot`
    inventory_snapshots_data = []
    for obj in data:
        stock = int(obj.get('stock', 0))
        price_with_tax = Decimal(obj.get('prixTtc', 0)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        weighted_average_price = Decimal(obj.get('prixMP', 0)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

        # Cap prices to a maximum value
        price_with_tax = min(price_with_tax, Decimal('99999999.99'))
        weighted_average_price = min(weighted_average_price, Decimal('99999999.99'))

        last_snapshot = latest_snapshots_map.get(obj['id'])

        # Check for changes against the latest snapshot
        if not last_snapshot or (
                last_snapshot['latest_stock'] != stock or last_snapshot['latest_price_with_tax'] != price_with_tax or
                last_snapshot['latest_weighted_average_price'] != weighted_average_price):
            inventory_snapshots_data.append(
                {'product_id': products_map[obj['id']].id, 'stock': stock, 'price_with_tax': price_with_tax,
                 'weighted_average_price': weighted_average_price, 'date': date.today()})

    # Create or update `InventorySnapshot`
    bulk_process(model=InventorySnapshot, data=inventory_snapshots_data, unique_fields=['product_id', 'date'],
                 update_fields=['stock', 'price_with_tax', 'weighted_average_price'])


def process_order_winpharma(pharmacy, data):
    """
    Process WinPharma order data to create or update suppliers, products, and orders in the database.

    :param pharmacy: Pharmacy instance associated with the data
    :param data: List of dictionaries representing order data
    """

    # Collect unique supplier codes and IDs
    supplier_data = []
    seen_suppliers = set()

    for obj in data:
        supplier_name = obj.get('nomFourn', obj.get('code_supplier', ''))
        supplier_key = (pharmacy.id, obj['codeFourn'])

        if supplier_key not in seen_suppliers:
            supplier_data.append({'pharmacy_id': pharmacy.id, 'code_supplier': obj['codeFourn'], 'name': supplier_name})
            seen_suppliers.add(supplier_key)

    # Create or update suppliers
    suppliers = bulk_process(model=Supplier, data=supplier_data, unique_fields=['pharmacy_id', 'code_supplier'],
                             update_fields=['name'])

    # Create a mapping of suppliers by their codes
    suppliers_map = {supplier.code_supplier: supplier for supplier in suppliers}

    # Extract all product IDs from the order lines
    product_ids = {line['prodId'] for obj in data for line in obj['produits']}

    # Récupérer les IDs des produits existants pour la pharmacie
    existing_products = InternalProduct.objects.filter(pharmacy_id=pharmacy.id, internal_id__in=product_ids)

    # Préparer les données des produits internes
    internal_product_data = []

    # Pour les produits existants, conserver les valeurs sans modification
    for product in existing_products:
        internal_product_data.append({
            'internal_id': product.internal_id,
            'pharmacy_id': pharmacy.id,
            'name': product.name  # Garder le nom existant
        })

    # Pour les produits non existants, attribuer 'empty' comme nom
    new_product_ids = set(product_ids) - set(existing_products.values_list('internal_id', flat=True))

    for product_id in new_product_ids:
        internal_product_data.append({
            'internal_id': product_id,
            'pharmacy_id': pharmacy.id,
            'name': 'empty'  # Attribuer 'empty' pour les nouveaux produits
        })

    # Create or update internal products
    internal_products = bulk_process(model=InternalProduct, data=internal_product_data,
                                     unique_fields=['pharmacy_id', 'internal_id'],
                                     update_fields=['name'])

    # Create a mapping of internal products by their IDs
    internal_products_map = {product.internal_id: product for product in internal_products}

    # Prepare order data
    order_data = []
    for obj in data:
        internal_id = int(obj['idCmd'].split('-')[0]) if isinstance(obj['idCmd'], str) else obj['idCmd']
        order_data.append(
            {'pharmacy_id': pharmacy.id, 'supplier_id': suppliers_map[obj['codeFourn']].id, 'internal_id': internal_id,
             'step': obj.get('etape'), 'sent_date': parse_date(obj.get('envoi')),
             'delivery_date': parse_date(obj.get('dateLivraison'), False), })

    # Create or update orders
    orders = bulk_process(
        model=Order,
        data=order_data,
        unique_fields=['internal_id', 'pharmacy_id'],
        update_fields=['supplier_id', 'step', 'sent_date', 'delivery_date']
    )

    # Create a mapping of orders by their internal IDs
    order_map = {order.internal_id: order for order in orders}

    # Prepare product-order mapping data
    product_order_data = []
    for obj in data:
        internal_id = int(obj['idCmd'].split('-')[0]) if isinstance(obj['idCmd'], str) else obj['idCmd']
        for line in obj['produits']:
            product_order_data.append(
                {'product': internal_products_map[line['prodId']], 'order': order_map[internal_id], 'qte': line['qte'],
                 'qte_ug': line['qteUG'], 'qte_a': line['qteA'], 'qte_r': line['qteR'],
                 'qte_ar': line['qteAReceptionner'], 'qte_ec': line['qteEC']})

    # Create or update product orders
    bulk_process(model=ProductOrder, data=product_order_data, unique_fields=['product', 'order'],
                 update_fields=['qte', 'qte_r', 'qte_a', 'qte_ug', 'qte_ec', 'qte_ar'])


def process_sales_winpharma(pharmacy, data):
    """
     Process sales records for a pharmacy and its products.
     """
    # Collect unique product IDs from the data

    code_13_refs = {obj.get('code13Ref') for obj in data if obj.get('code13Ref')}
    with transaction.atomic():
        global_products_map = {gp.code_13_ref: gp for gp in GlobalProduct.objects.filter(code_13_ref__in=code_13_refs)}
        missing_refs = code_13_refs - global_products_map.keys()
        if missing_refs:
            GlobalProduct.objects.bulk_create(
                [GlobalProduct(code_13_ref=ref, name='Default Name') for ref in missing_refs])
            global_products_map.update(
                {gp.code_13_ref: gp for gp in GlobalProduct.objects.filter(code_13_ref__in=missing_refs)})

    # Préparer un ensemble des IDs des produits à traiter
    product_ids = {str(obj['prodId']) for obj in data}

    # Récupérer les produits existants pour la pharmacie
    existing_products = InternalProduct.objects.filter(pharmacy_id=pharmacy.id, internal_id__in=product_ids)

    # Récupérer les IDs des produits existants
    existing_product_ids = set(existing_products.values_list('internal_id', flat=True))

    # Préparer les données des produits internes
    internal_product_data = []

    # Ajouter les produits existants à la liste (sans modification)
    for product in existing_products:
        internal_product_data.append({
            'internal_id': str(product.internal_id),
            'pharmacy_id': pharmacy.id,
            'name': product.name,  # Garder le nom existant
            'code_13_ref': product.code_13_ref,  # Garder la référence 13
            'TVA': product.TVA  # Garder la TVA
        })

    # Ajouter les produits non existants avec 'empty' comme nom
    for obj in data:
        if obj['prodId'] not in existing_product_ids:
            code_13_ref = obj.get('code13Ref') or None
            global_product_instance = global_products_map.get(code_13_ref)

            internal_product_data.append({
                'pharmacy_id': pharmacy.id,
                'internal_id': str(obj['prodId']),
                'code_13_ref': global_product_instance,
                'name': obj.get('nom', ''),  # Si 'nom' est vide, prendre une chaîne vide
                'TVA': obj.get('TVA', 0.0),  # Prendre la TVA, sinon 0.0
            })

    # Créer ou mettre à jour les produits internes
    products = bulk_process(
        model=InternalProduct,
        data=internal_product_data,
        unique_fields=['pharmacy_id', 'internal_id'],
        update_fields=['code_13_ref', 'name', 'TVA']
    )

    # Créer un mapping des produits créés/mis à jour
    products_map = {str(product.internal_id): product for product in products}

    sales_data = []
    for obj in data:
        snapshot = products_map[str(obj['prodId'])].snapshot_history.order_by('-created_at').first()
        if snapshot:
            sales_data.append({'product': snapshot, 'time': parse_date(obj['heure']), 'operator_code': obj['codeOperateur'],
                               'quantity': obj['qte']})

    # Process sales in chunks
    bulk_process(model=Sales, data=sales_data, unique_fields=['product', 'time', 'operator_code'],
                 update_fields=['quantity'])


def process_stock_dexter(pharmacy, data, date):
    """
    Crée ou met à jour des produits et leurs snapshots en utilisant bulk_process.
    """
    internal_products_data = []
    for obj in data:
        produit_id = str(obj['produit_id'])
        libelle_produit = str(obj['libelle_produit']) if obj['libelle_produit'] else ""
        tva = float(obj['taux_Tva']) if obj.get('taux_Tva') is not None else 0.0

        internal_products_data.append({
            'pharmacy_id': pharmacy.id,
            'internal_id': produit_id,
            'name': libelle_produit,
            'TVA': tva
        })

    # Créer ou mettre à jour les produits
    try:
        products = bulk_process(model=InternalProduct, data=internal_products_data,
                                unique_fields=['pharmacy_id', 'internal_id'], update_fields=['name', 'TVA'])
    except Exception as e:
        logger.error(f"Error during bulk process of products: {e}")
        raise

    # Récupérer les produits créés/mis à jour pour lier aux snapshots
    products_map = {str(product.internal_id): product for product in products}
    date = parse_date(date, False)

    # Optimisation des requêtes pour obtenir les derniers snapshots
    latest_snapshots = InventorySnapshot.objects.filter(
        product_id__in=[product.id for product in products]
    ).values(
        'product_id', 'product__internal_id'
    ).annotate(
        latest_date=Max('date'),
        latest_stock=F('stock'),
        latest_price_with_tax=F('price_with_tax'),
        latest_weighted_average_price=F('weighted_average_price')
    )

    # Créer un mapping des derniers snapshots par produit
    latest_snapshots_map = {snapshot['product__internal_id']: snapshot for snapshot in latest_snapshots}

    # Préparer les données pour InventorySnapshot
    inventory_snapshots_data = []
    for obj in data:
        # Récupération des valeurs depuis les données
        prix_achat = obj.get('px_achat_PMP_HT', 0.0)  # Convertir en flottant
        if prix_achat is None or prix_achat == '':
            prix_achat = 0.0  # ou assignez une autre valeur par défaut si nécessaire
        try:
            stock = int(obj['qte_stock']) if obj.get('qte_stock') else 0
        except ValueError:
            stock = 0
            logger.warning(f"Invalid stock value for product {obj['produit_id']}: {obj['qte_stock']}")

        price_with_tax = Decimal(obj['px_vte_TTC']).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if obj.get('px_vte_TTC') else Decimal('0.00')
        weighted_average_price = Decimal(prix_achat).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

        # Récupérer le dernier snapshot pour ce produit
        last_snapshot = latest_snapshots_map.get(str(obj['produit_id']))

        # Vérifier les changements par rapport au dernier snapshot
        if not last_snapshot or (
                last_snapshot['latest_stock'] != stock or last_snapshot['latest_price_with_tax'] != price_with_tax or
                last_snapshot['latest_weighted_average_price'] != weighted_average_price):
            # Ajouter un nouveau snapshot dans les données à créer
            inventory_snapshots_data.append(
                {'product_id': products_map[str(obj['produit_id'])].id, 'stock': stock,
                 'price_with_tax': price_with_tax,
                 'weighted_average_price': weighted_average_price, 'date': date})

    # Créer ou mettre à jour les snapshots
    try:
        bulk_process(model=InventorySnapshot, data=inventory_snapshots_data, unique_fields=['product_id', 'date'],
                     update_fields=['stock', 'price_with_tax', 'weighted_average_price'])
    except Exception as e:
        logger.error(f"Error during bulk process of inventory snapshots: {e}")
        raise


def process_achat_dexter(pharmacy, data):
    """
    Create or update orders and their associated product lines using bulk_process.

    :param pharmacy: Pharmacy instance to associate with the data
    :param data: List of dictionaries representing Dexter order data
    """
    # Step 1: Prepare supplier data and identify duplicates
    supplier_data = []
    seen_suppliers = set()

    for obj in data:
        supplier_name = obj.get('libelle_fournisseur', obj.get('id_fournisseur', ''))
        supplier_key = (pharmacy.id, obj['id_fournisseur'])

        # Check for duplicate suppliers
        if supplier_key not in seen_suppliers:
            supplier_data.append({
                'pharmacy_id': pharmacy.id,
                'code_supplier': str(obj['id_fournisseur']),
                'name': str(supplier_name)  # Ensure name is a string
            })
            seen_suppliers.add(supplier_key)

    # Create or update suppliers in the database
    try:
        suppliers = bulk_process(
            model=Supplier,
            data=supplier_data,
            unique_fields=['pharmacy_id', 'code_supplier'],
            update_fields=['name']
        )
    except Exception as e:
        logger.error(f"Erreur lors du traitement des fournisseurs: {e}")
        raise

    # Map suppliers for quick lookup
    supplier_map = {str(supplier.code_supplier): supplier for supplier in suppliers}

    # Step 2: Extract and prepare internal product data
    product_ids = {str(line['produit_id']) for obj in data for line in obj['lignes']}

    existing_products = InternalProduct.objects.filter(pharmacy_id=pharmacy.id, internal_id__in=product_ids)
    internal_product_data = []

    # Process existing products
    for product in existing_products:
        internal_product_data.append({
            'internal_id': str(product.internal_id),
            'pharmacy_id': pharmacy.id,
            'name': product.name  # Keep existing name
        })

    # Process new products
    new_product_ids = set(product_ids) - set(existing_products.values_list('internal_id', flat=True))
    for product_id in new_product_ids:
        internal_product_data.append({
            'internal_id': str(product_id),
            'pharmacy_id': pharmacy.id,
            'name': 'empty'
        })

    # Create or update internal products
    try:
        internal_products = bulk_process(
            model=InternalProduct,
            data=internal_product_data,
            unique_fields=['pharmacy_id', 'internal_id'],
            update_fields=['name']
        )
    except Exception as e:
        logger.error(f"Erreur lors du traitement des produits internes: {e}")
        raise

    internal_products_map = {str(product.internal_id): product for product in internal_products}

    # Step 3: Prepare order data
    order_data = []
    for obj in data:
        try:
            order_data.append({
                'internal_id': str(obj['commande_id']),
                'pharmacy_id': pharmacy.id,
                'supplier_id': supplier_map[str(obj['id_fournisseur'])].id,
                'step': str(obj['etat_commande']),  # Ensure step is a string
                'sent_date': parse_date(obj.get('date_transmission')),
                'delivery_date': parse_date(obj.get('date_reception'), False)
            })
        except KeyError as e:
            logger.warning(f"Clé manquante dans la commande {obj['commande_id']}: {e}")
            continue
        except Exception as e:
            logger.error(f"Erreur lors du traitement de la commande {obj['commande_id']}: {e}")
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
        logger.error(f"Erreur lors du traitement des commandes: {e}")
        raise

    # Map orders for quick lookup
    order_map = {str(order.internal_id): order for order in orders}  # Forcer les IDs en chaîne

    # Étape 4 : Préparer les données d'association produit-commande
    product_order_data = []
    existing_associations = set()  # Un set pour garder les associations existantes

    for obj in data:
        for line in obj['lignes']:
            # Assurez-vous que les quantités sont des entiers
            quantity_ordered = int(line.get('qte_cde', 0))
            quantity_received = int(line.get("total_recu", 0))

            # Création d'une clé unique (order_id, product_id)
            order_id = str(obj['commande_id'])
            product_id = str(line['produit_id'])

            # Vérifiez si cette combinaison existe déjà
            if (order_id, product_id) in existing_associations:
                continue  # Si l'association existe déjà, ignorer cette entrée

            # Ajouter l'association à la liste et au set
            existing_associations.add((order_id, product_id))

            # Calculer les quantités restantes et autres quantités associées
            product_order_data.append({
                'product': internal_products_map[product_id],
                'order': order_map[order_id],
                'qte': quantity_ordered,
                'qte_a': quantity_ordered,
                'qte_ug': line['total_ug_liv'],
                'qte_r': quantity_received,
                'qte_ec': quantity_ordered - quantity_received,
                'qte_ar': quantity_ordered - quantity_received
            })

    # Créer ou mettre à jour les associations produit-commande
    try:
        bulk_process(
            model=ProductOrder,
            data=product_order_data,
            unique_fields=['order', 'product'],
            update_fields=['qte', 'qte_r', 'qte_a', 'qte_ug', 'qte_ec', 'qte_ar']
        )
    except Exception as e:
        logger.error(f"Erreur lors du traitement des associations produit-commande: {e}")
        raise


def process_vente_dexter(pharmacy, data):
    """
    Crée ou met à jour les ventes et leurs produits associés en utilisant bulk_process.
    """

    # Extraire tous les IDs des produits à partir des lignes de commande
    internal_product_data = []
    seen_products = set()  # Utilisé pour éviter les doublons

    for obj in data:
        for invoice in obj['factures']:
            for line in invoice['lignes_de_facture']:
                # Vérifier que 'produit_id' est valide
                if not line.get('produit_id'):
                    continue  # Ignorer les lignes sans produit_id

                # Crée une clé unique basée sur 'produit_id' et 'pharmacy_id'
                unique_key = (str(line['produit_id']), pharmacy.id)

                # Ajouter uniquement si la clé n'a pas encore été vue
                if unique_key not in seen_products:
                    seen_products.add(unique_key)
                    internal_product_data.append({
                        'internal_id': str(line['produit_id']),
                        'pharmacy_id': pharmacy.id,
                        'name': line['libelle_produit'] if line.get('libelle_produit') else 'Unknown'
                    })

    # Vous pouvez aussi ajouter d'autres informations comme 'name', 'TVA', etc.
    try:
        internal_products = bulk_process(
            model=InternalProduct,
            data=internal_product_data,
            unique_fields=['pharmacy_id', 'internal_id'],
            update_fields=['name']  # Ajouter des champs supplémentaires si nécessaire
        )
    except Exception as e:
        logger.error(f"Erreur lors du traitement des produits internes: {e}")
        raise

    product_ids = [product.id for product in internal_products]

    latest_snapshots = (
        InventorySnapshot.objects
        .filter(product=OuterRef('id'))  # Relation correcte avec le modèle InventorySnapshot
        .order_by('-created_at')
        .values('id')[:1]  # Sélectionne uniquement le dernier ID
    )

    internal_products = InternalProduct.objects.filter(id__in=product_ids).annotate(
        latest_snapshot_id=Subquery(latest_snapshots)
    )

    # Créer un mapping des produits internes pour les associer rapidement aux lignes de commande
    internal_products_map = {str(product.internal_id): product.latest_snapshot_id for product in internal_products}

    sales_data = []
    for obj in data:
        for invoice in obj['factures']:
            for line in invoice['lignes_de_facture']:
                if not line.get('produit_id'):
                    continue  # Ignorer les lignes sans produit_id

                # Si le produit n'a pas de snapshot associé, ignorer
                snapshot_id = internal_products_map.get(str(line['produit_id']))
                if not snapshot_id:
                    continue

                # Assurez-vous que la quantité est un entier
                try:
                    quantity = int(line.get('quantite', 0))
                except ValueError:
                    quantity = 0  # Si la quantité est invalide, la considérer comme 0

                # Ajouter les données de vente
                sales_data.append({
                    'product_id': snapshot_id,
                    'quantity': quantity,
                    'time': parse_date(obj['date_acte']),
                    'operator_code': str(obj['code_operateur'])
                })

    # Process sales in chunks
    try:
        bulk_process(
            model=Sales,
            data=sales_data,
            unique_fields=['product_id', 'time', 'operator_code'],
            update_fields=['quantity']
        )
    except Exception as e:
        logger.error(f"Erreur lors du traitement des ventes: {e}")
        raise
