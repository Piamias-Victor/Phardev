import os
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Any, Optional
import requests
from requests.exceptions import RequestException, Timeout

from django.db import transaction
from django.db.models import OuterRef, Subquery, F, Window
from django.db.models.functions import RowNumber
from django.utils import timezone

from data.models import (
    GlobalProduct, InternalProduct, InventorySnapshot, 
    Order, ProductOrder, Supplier, Sales
)
from data.services import common

logger = logging.getLogger(__name__)


class ApothicalAPIClient:
    """Client API pour Smart-RX PharmaCloud (Apothical)"""
    
    def __init__(self):
        self.base_url = "https://www.pharmanuage.fr/data-api/v2"
        self.username = os.environ.get('APOTHICAL_USERNAME')
        self.password = os.environ.get('APOTHICAL_PASSWORD')
        self.token = None
        self.token_expires = None
    
    def authenticate(self) -> bool:
        """Authentification et récupération du token Bearer"""
        if not self.username or not self.password:
            logger.error("APOTHICAL_USERNAME et APOTHICAL_PASSWORD requis")
            return False
        
        # Vérifier si le token est encore valide (valid 24h)
        if self.token and self.token_expires and datetime.now() < self.token_expires:
            return True
            
        auth_url = f"{self.base_url}/auth"
        payload = {
            "username": self.username,
            "password": self.password
        }
        
        try:
            response = requests.post(auth_url, json=payload, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if 'token' in data:
                    self.token = data['token']
                    # Token valide 24h
                    self.token_expires = datetime.now() + timedelta(hours=23, minutes=30)
                    logger.info("Authentification Apothical réussie")
                    return True
                else:
                    logger.error(f"Token non trouvé dans la réponse: {data}")
                    return False
            else:
                logger.error(f"Échec authentification Apothical: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Erreur authentification Apothical: {e}")
            return False
    
    def get_headers(self) -> Dict[str, str]:
        """Headers avec authentification Bearer"""
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
    
    def fetch_paginated_data(self, endpoint: str, finess: str, params: Dict = None) -> List[Dict]:
        """Récupère toutes les données paginées d'un endpoint"""
        if not self.authenticate():
            return []
        
        all_data = []
        page = 0
        page_size = 100  # Maximum autorisé
        
        base_params = params or {}
        
        while True:
            current_params = {
                **base_params,
                "page": page,
                "size": page_size
            }
            
            url = f"{self.base_url}/{finess}/{endpoint}"
            
            try:
                response = requests.get(
                    url, 
                    headers=self.get_headers(), 
                    params=current_params, 
                    timeout=60
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # L'API Apothical retourne un format paginé avec _embedded
                    if isinstance(data, dict) and '_embedded' in data:
                        # Extraire les données de _embedded selon l'endpoint
                        if endpoint == 'products' and 'products' in data['_embedded']:
                            page_data = data['_embedded']['products']
                        elif endpoint == 'orders' and 'orders' in data['_embedded']:
                            page_data = data['_embedded']['orders']
                        elif endpoint == 'sales' and 'sales' in data['_embedded']:
                            page_data = data['_embedded']['sales']
                        else:
                            logger.warning(f"Structure _embedded inattendue pour {endpoint}")
                            break
                        
                        if not page_data:  # Page vide = fin
                            break
                        
                        all_data.extend(page_data)
                        
                        if len(page_data) < page_size:  # Dernière page
                            break
                        
                        page += 1
                        
                    elif isinstance(data, list):
                        # Format liste directe (fallback)
                        if not data:
                            break
                        all_data.extend(data)
                        
                        if len(data) < page_size:
                            break
                        
                        page += 1
                    else:
                        logger.warning(f"Format inattendu pour {endpoint}: {type(data)}")
                        break
                        
                elif response.status_code == 403:
                    logger.warning(f"Accès refusé pour {endpoint} FINESS {finess}")
                    break
                else:
                    logger.error(f"Erreur {response.status_code} pour {endpoint}: {response.text}")
                    break
                    
            except Timeout:
                logger.warning(f"Timeout pour {endpoint} page {page}")
                break
            except Exception as e:
                logger.error(f"Erreur récupération {endpoint}: {e}")
                break
        
        logger.info(f"Récupéré {len(all_data)} éléments pour {endpoint}")
        return all_data


def process_products(pharmacy, finess: str):
    """
    Traite les produits Apothical pour créer/mettre à jour les produits et snapshots
    
    Args:
        pharmacy: Instance Pharmacy
        finess: Code FINESS de la pharmacie
    
    Returns:
        Dict avec les résultats du traitement
    """
    client = ApothicalAPIClient()
    
    # Récupération des produits via API
    products_data = client.fetch_paginated_data("products", finess)
    
    if not products_data:
        logger.warning(f"Aucun produit récupéré pour FINESS {finess}")
        return {"products": [], "snapshots": []}
    
    # Pré-traitement des données
    preprocessed_data = []
    
    for product in products_data:
        try:
            # Extraction des données importantes
            product_id = product.get('productId')
            if not product_id or int(product_id) < 0:
                continue
            
            # Code officiel (CIP/EAN)
            official_code = product.get('officialProductCode') or product.get('ean13')
            
            # TVA (déjà en décimal dans l'API)
            vat_rate = product.get('vatRate', 0)
            tva_converted = Decimal(str(vat_rate)) if vat_rate else Decimal('0')
            
            # Prix
            selling_price = product.get('sellingPrice', 0)
            price_ttc = min(
                Decimal(str(selling_price)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
                Decimal("99999999.99")
            ) if selling_price else Decimal('0.00')
            
            # Prix moyen pondéré
            avg_cost = product.get('averageTotalCost', 0)
            weighted_price = min(
                Decimal(str(avg_cost)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
                Decimal("99999999.99")
            ) if avg_cost else Decimal('0.00')
            
            # Stock
            stock_qty = product.get('stockQuantity', 0)
            stock_val = common.clamp(int(stock_qty) if stock_qty else 0, -32768, 32767)
            
            preprocessed_data.append({
                "product_id": str(product_id),
                "name": product.get('description', ''),
                "code_13_ref": official_code,
                "TVA": tva_converted,
                "stock": stock_val,
                "price_with_tax": price_ttc,
                "weighted_average_price": weighted_price,
            })
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Produit Apothical ignoré ({product.get('productId')}): {e}")
            continue
    
    if not preprocessed_data:
        return {"products": [], "snapshots": []}
    
    # Traitement des GlobalProduct
    refs = {obj["code_13_ref"] for obj in preprocessed_data if obj["code_13_ref"]}
    
    with transaction.atomic():
        existing_globals = GlobalProduct.objects.filter(code_13_ref__in=refs).only("code_13_ref", "name")
        gp_map = {gp.code_13_ref: gp for gp in existing_globals}
        
        missing = refs - gp_map.keys()
        if missing:
            GlobalProduct.objects.bulk_create([
                GlobalProduct(code_13_ref=ref, name="Produit Apothical") 
                for ref in missing
            ])
            gp_map.update({
                gp.code_13_ref: gp 
                for gp in GlobalProduct.objects.filter(code_13_ref__in=missing)
            })
    
    # Traitement des InternalProduct
    ip_rows = [{
        "pharmacy_id": pharmacy.id,
        "internal_id": obj["product_id"],
        "code_13_ref": gp_map.get(obj["code_13_ref"]),
        "name": obj["name"],
        "TVA": obj["TVA"],
    } for obj in preprocessed_data]
    
    products = common.bulk_process(
        model=InternalProduct,
        data=ip_rows,
        unique_fields=["pharmacy_id", "internal_id"],
        update_fields=["code_13_ref", "name", "TVA"],
    )
    
    p_map = {str(p.internal_id): p for p in products}
    
    # Traitement des InventorySnapshot
    latest = (
        InventorySnapshot.objects.filter(product_id__in=[p.id for p in products])
        .annotate(
            row_number=Window(
                expression=RowNumber(), 
                partition_by=F("product_id"), 
                order_by=F("date").desc()
            )
        )
        .filter(row_number=1)
        .values("product_id", "stock", "price_with_tax", "weighted_average_price")
    )
    latest_map = {s["product_id"]: s for s in latest}
    
    snapshots = []
    for obj in preprocessed_data:
        p = p_map.get(obj["product_id"])
        if not p:
            continue
            
        last = latest_map.get(p.id)
        if (
            not last
            or last["stock"] != obj["stock"]
            or last["price_with_tax"] != obj["price_with_tax"]
            or last["weighted_average_price"] != obj["weighted_average_price"]
        ):
            snapshots.append({
                "product_id": p.id,
                "stock": obj["stock"],
                "price_with_tax": obj["price_with_tax"],
                "weighted_average_price": obj["weighted_average_price"],
                "date": date.today(),
            })
    
    if snapshots:
        common.bulk_process(
            model=InventorySnapshot,
            data=snapshots,
            unique_fields=["product_id", "date"],
            update_fields=["stock", "price_with_tax", "weighted_average_price"],
        )
    
    return {"products": products, "snapshots": snapshots}


def process_orders(pharmacy, finess: str):
    """
    Traite les commandes Apothical
    
    Args:
        pharmacy: Instance Pharmacy
        finess: Code FINESS de la pharmacie
    
    Returns:
        Dict avec les résultats du traitement
    """
    client = ApothicalAPIClient()
    
    # Récupération des commandes récentes (dernière semaine)
    from_date = (datetime.now() - timedelta(days=7)).isoformat()
    params = {"from": from_date}
    
    orders_data = client.fetch_paginated_data("orders", finess, params)
    
    if not orders_data:
        logger.info(f"Aucune commande récente pour FINESS {finess}")
        return {"suppliers": [], "products": [], "orders": [], "product_orders": []}
    
    # Pré-traitement des données
    preprocessed_data = []
    
    for order in orders_data:
        try:
            order_number = order.get('orderNumber')
            if not order_number:
                continue
            
            # Informations supplier
            supplier_data = order.get('supplier', {})
            supplier_id = supplier_data.get('supplierId') or supplier_data.get('softwareSupplierCode', 'unknown')
            supplier_name = supplier_data.get('name', f'Fournisseur {supplier_id}')
            
            # Informations commande
            status = order.get('orderStatus', '')
            invoice_data = order.get('ordersInvoice', {})
            
            sent_date = common.parse_date(invoice_data.get('transmissionDate'))
            delivery_date = common.parse_date(invoice_data.get('deliveryDate'), False)
            
            # Lignes de commande
            order_lines = invoice_data.get('orderLines', [])
            products = []
            
            for line in order_lines:
                try:
                    product_data = line.get('product', {})
                    product_id = product_data.get('productId')
                    
                    if not product_id or int(product_id) < 0:
                        continue
                    
                    qty_ordered = line.get('quantityOrdered', 0)
                    qty_delivered = line.get('quantityDelivered', 0)
                    qty_missing = line.get('missingQty', 0)
                    
                    products.append({
                        "product_id": str(product_id),
                        "qte": common.clamp(int(qty_ordered), -32768, 32767),
                        "qte_r": common.clamp(int(qty_delivered), -32768, 32767),
                        "qte_a": common.clamp(int(qty_ordered), -32768, 32767),
                        "qte_ug": 0,  # Non fourni par l'API
                        "qte_ec": common.clamp(int(qty_missing), -32768, 32767),
                        "qte_ar": common.clamp(int(qty_missing), -32768, 32767),
                    })
                    
                except (ValueError, TypeError) as e:
                    logger.warning(f"Ligne commande ignorée: {e}")
                    continue
            
            preprocessed_data.append({
                "order_id": str(order_number),
                "supplier_id": str(supplier_id),
                "supplier_name": supplier_name,
                "step": status,
                "sent_date": sent_date,
                "delivery_date": delivery_date,
                "products": products,
            })
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Commande Apothical ignorée ({order.get('orderNumber')}): {e}")
            continue
    
    if not preprocessed_data:
        return {"suppliers": [], "products": [], "orders": [], "product_orders": []}
    
    # Traitement des fournisseurs
    supplier_data = []
    seen_suppliers = set()
    
    for obj in preprocessed_data:
        key = (pharmacy.id, obj["supplier_id"])
        if key not in seen_suppliers:
            supplier_data.append({
                "pharmacy_id": pharmacy.id,
                "code_supplier": obj["supplier_id"],
                "name": obj["supplier_name"],
            })
            seen_suppliers.add(key)
    
    suppliers = common.bulk_process(
        model=Supplier,
        data=supplier_data,
        unique_fields=["pharmacy_id", "code_supplier"],
        update_fields=["name"],
    )
    suppliers_map = {s.code_supplier: s for s in suppliers}
    
    # Traitement des produits internes
    product_ids = {line["product_id"] for obj in preprocessed_data for line in obj["products"]}
    
    existing_products = InternalProduct.objects.filter(
        pharmacy_id=pharmacy.id,
        internal_id__in=product_ids,
    )
    
    internal_product_data = [{
        "internal_id": p.internal_id,
        "pharmacy_id": pharmacy.id,
        "name": p.name,
    } for p in existing_products]
    
    new_ids = product_ids - set(existing_products.values_list("internal_id", flat=True))
    internal_product_data.extend([{
        "internal_id": pid,
        "pharmacy_id": pharmacy.id,
        "name": "Produit Apothical",
    } for pid in new_ids])
    
    products = common.bulk_process(
        model=InternalProduct,
        data=internal_product_data,
        unique_fields=["pharmacy_id", "internal_id"],
        update_fields=["name"],
    )
    products_map = {p.internal_id: p for p in products}
    
    # Traitement des commandes
    order_rows = [{
        "internal_id": obj["order_id"],
        "pharmacy_id": pharmacy.id,
        "supplier_id": suppliers_map[obj["supplier_id"]].id,
        "step": obj["step"],
        "sent_date": obj["sent_date"],
        "delivery_date": obj["delivery_date"],
    } for obj in preprocessed_data if obj["supplier_id"] in suppliers_map]
    
    orders = common.bulk_process(
        model=Order,
        data=order_rows,
        unique_fields=["internal_id", "pharmacy_id"],
        update_fields=["supplier_id", "step", "sent_date", "delivery_date"],
    )
    orders_map = {o.internal_id: o for o in orders}
    
    # Traitement des liaisons produit-commande
    product_order_rows = []
    
    for obj in preprocessed_data:
        order = orders_map.get(obj["order_id"])
        if not order:
            continue
        
        for line in obj["products"]:
            product = products_map.get(line["product_id"])
            if not product:
                continue
            
            product_order_rows.append({
                "product": product,
                "order": order,
                "qte": line["qte"],
                "qte_r": line["qte_r"],
                "qte_a": line["qte_a"],
                "qte_ug": line["qte_ug"],
                "qte_ec": line["qte_ec"],
                "qte_ar": line["qte_ar"],
            })
    
    if product_order_rows:
        common.bulk_process(
            model=ProductOrder,
            data=product_order_rows,
            unique_fields=["order", "product"],
            update_fields=["qte", "qte_r", "qte_a", "qte_ug", "qte_ec", "qte_ar"],
        )
    
    return {
        "suppliers": suppliers,
        "products": products,
        "orders": orders,
        "product_orders": product_order_rows,
    }


def process_sales(pharmacy, finess: str):
    """
    Traite les ventes Apothical
    
    Args:
        pharmacy: Instance Pharmacy
        finess: Code FINESS de la pharmacie
    
    Returns:
        None
    """
    client = ApothicalAPIClient()
    
    # Récupération des ventes récentes (dernière semaine)
    from_date = (datetime.now() - timedelta(days=7)).isoformat()
    params = {"from": from_date}
    
    sales_data = client.fetch_paginated_data("sales", finess, params)
    
    if not sales_data:
        logger.info(f"Aucune vente récente pour FINESS {finess}")
        return
    
    # Pré-traitement des données
    preprocessed_data = []
    
    for sale in sales_data:
        try:
            sale_date = sale.get('date')
            sale_lines = sale.get('saleLines', [])
            
            for line in sale_lines:
                try:
                    product_id = line.get('productId')
                    if not product_id or int(product_id) < 0:
                        continue
                    
                    quantity = line.get('quantitySold', 0)
                    if not quantity:
                        continue
                    
                    preprocessed_data.append({
                        "product_id": str(product_id),
                        "date": sale_date,
                        "qte": int(quantity),
                    })
                    
                except (ValueError, TypeError) as e:
                    logger.warning(f"Ligne vente ignorée: {e}")
                    continue
                    
        except (ValueError, TypeError) as e:
            logger.warning(f"Vente Apothical ignorée: {e}")
            continue
    
    if not preprocessed_data:
        return
    
    # Récupération des snapshots associés
    product_ids = {obj["product_id"] for obj in preprocessed_data}
    
    latest_snapshot_sub = (
        InventorySnapshot.objects.filter(product=OuterRef("id"))
        .order_by("-created_at")
        .values("id")[:1]
    )
    
    products = (
        InternalProduct.objects.filter(pharmacy=pharmacy, internal_id__in=product_ids)
        .annotate(latest_snapshot_id=Subquery(latest_snapshot_sub))
    )
    snapshot_map = {str(p.internal_id): p.latest_snapshot_id for p in products}
    
    # Agrégation des quantités par produit et date
    aggregated_sales = {}
    for obj in preprocessed_data:
        product_id = obj["product_id"]
        sale_date = common.parse_date(obj["date"], False)
        key = (product_id, sale_date)
        
        if key not in aggregated_sales:
            aggregated_sales[key] = obj["qte"]
        else:
            aggregated_sales[key] += obj["qte"]
    
    # Construction des données de vente
    sales_rows = []
    for (product_id, sale_date), total_qty in aggregated_sales.items():
        snapshot_id = snapshot_map.get(product_id)
        if not snapshot_id:
            continue
        
        sales_rows.append({
            "product_id": snapshot_id,
            "quantity": common.clamp(total_qty, -32768, 32767),
            "date": sale_date,
        })
    
    if not sales_rows:
        return
    
    # Insertion en base
    try:
        common.bulk_process(
            model=Sales,
            data=sales_rows,
            unique_fields=["product_id", "date"],
            update_fields=["quantity"],
        )
    except Exception as e:
        logger.error(f"Erreur traitement ventes Apothical: {e}")
        raise