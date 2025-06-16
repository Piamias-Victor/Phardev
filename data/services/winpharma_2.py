from datetime import date
from decimal import Decimal, ROUND_HALF_UP
import logging
from typing import Any, Dict, List, Set, cast

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
    preprocessed: List[Dict[str, Any]] = []
    for block in data:
        for obj in block.get("produits", []):
            try:
                prod_id_raw = obj.get("ProdId")
                if prod_id_raw is None or int(prod_id_raw) < 0:
                    continue

                # -- TVA ------------------------------------------------
                tva_value = Decimal(str(obj.get("TVA", 0) or 0))
                tva_converted = (tva_value / Decimal(100)) if tva_value > 1 else tva_value

                # -- Prix ---------------------------------------------
                price_ttc = min(
                    Decimal(str(obj.get("PrixTTC", 0) or 0)).quantize(
                        Decimal("0.01"), rounding=ROUND_HALF_UP
                    ),
                    Decimal("99999999.99"),
                )
                weighted_price = min(
                    Decimal(str(obj.get("PrixMP", 0) or 0)).quantize(
                        Decimal("0.01"), rounding=ROUND_HALF_UP
                    ),
                    Decimal("99999999.99"),
                )

                stock_val = common.clamp(int(obj.get("Stock", 0) or 0), -32768, 32767)

                preprocessed.append(
                    {
                        "product_id": str(prod_id_raw),
                        "name": obj.get("Nom", ""),
                        "code_13_ref": obj.get("Code13Ref") or None,
                        "TVA": tva_converted,
                        "stock": stock_val,
                        "price_with_tax": price_ttc,
                        "weighted_average_price": weighted_price,
                    }
                )
            except (ValueError, TypeError) as err:
                logger.warning("Produit ignoré (%s) : %s", obj.get("ProdId", "unknown"), err)
                continue

    if not preprocessed:
        return {"products": [], "snapshots": []}

        # ------------------------------------------------------------------
        # 2. GlobalProduct ---------------------------------------------------
        # ------------------------------------------------------------------
    refs = {o["code_13_ref"] for o in preprocessed if o["code_13_ref"]}

    with transaction.atomic():
        existing_globals = GlobalProduct.objects.filter(code_13_ref__in=refs).only("code_13_ref", "name")
        gp_map = {gp.code_13_ref: gp for gp in existing_globals}

        missing = refs - gp_map.keys()
        if missing:
            GlobalProduct.objects.bulk_create(
                [GlobalProduct(code_13_ref=ref, name="Default Name") for ref in missing]
            )
            gp_map.update({gp.code_13_ref: gp for gp in GlobalProduct.objects.filter(code_13_ref__in=missing)})

    # ------------------------------------------------------------------
    # 3. InternalProduct -------------------------------------------------
    # ------------------------------------------------------------------
    ip_rows = [
        {
            "pharmacy_id": pharmacy.id,
            "internal_id": o["product_id"],
            "code_13_ref": gp_map.get(o["code_13_ref"]),
            "name": o["name"],
            "TVA": o["TVA"],
        }
        for o in preprocessed
    ]

    products = cast(
        List[InternalProduct],
        common.bulk_process(
            model=InternalProduct,
            data=ip_rows,
            unique_fields=["pharmacy_id", "internal_id"],
            update_fields=["code_13_ref", "name", "TVA"],
        ),
    )
    p_map = {str(p.internal_id): p for p in products}

    # ------------------------------------------------------------------
    # 4. InventorySnapshot ----------------------------------------------
    # ------------------------------------------------------------------
    latest = (
        InventorySnapshot.objects.filter(product_id__in=[p.id for p in products])
        .annotate(
            row_number=Window(
                expression=RowNumber(), partition_by=F("product_id"), order_by=F("date").desc()
            )
        )
        .filter(row_number=1)
        .values("product_id", "stock", "price_with_tax", "weighted_average_price")
    )
    latest_map = {s["product_id"]: s for s in latest}

    snapshots: List[Dict[str, Any]] = []
    for o in preprocessed:
        p = p_map.get(o["product_id"])
        if not p:
            continue
        last = latest_map.get(p.id)
        if (
                not last
                or last["stock"] != o["stock"]
                or last["price_with_tax"] != o["price_with_tax"]
                or last["weighted_average_price"] != o["weighted_average_price"]
        ):
            snapshots.append(
                {
                    "product_id": p.id,
                    "stock": o["stock"],
                    "price_with_tax": o["price_with_tax"],
                    "weighted_average_price": o["weighted_average_price"],
                    "date": date.today(),
                }
            )

    if snapshots:
        common.bulk_process(
            model=InventorySnapshot,
            data=snapshots,
            unique_fields=["product_id", "date"],
            update_fields=["stock", "price_with_tax", "weighted_average_price"],
        )

    # ------------------------------------------------------------------
    # 5. Résultat --------------------------------------------------------
    # ------------------------------------------------------------------
    return {"products": products, "snapshots": snapshots}


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
    for block in data:
        for achat in block.get("achats", []):
            try:
                order_id_raw = achat.get("id")
                order_id = int(order_id_raw) if order_id_raw is not None else None
                if order_id is None:
                    raise ValueError("Champ 'id' manquant")

                supplier_id = achat.get("codeFourn")
                if not supplier_id:
                    raise ValueError("Champ 'codeFourn' manquant")
                supplier_name = achat.get("nomFourn", supplier_id)

                step = achat.get("channel", "")  # plus de champ 'etape'
                sent_date = common.parse_date(achat.get("dateEnvoi"))
                delivery_date = common.parse_date(achat.get("dateLivraison"), False)

                # -------- lignes produits --------------------------------
                products: List[Dict[str, Any]] = []
                for line in achat.get("lignes", []):
                    try:
                        prod_id = int(line.get("prodId"))
                        if prod_id < 0:
                            # Id négatif = placeholder, on ignore
                            continue

                        product_id = str(prod_id)
                        qte_c = common.clamp(int(line.get("qteC", 0)), -32768, 32767)
                        qte_r = common.clamp(int(line.get("qteR", 0)), -32768, 32767)
                        qte_ug = common.clamp(int(line.get("qteUG", 0)), -32768, 32767)
                        qte_ec = common.clamp(int(line.get("qteEC", 0)), -32768, 32767)

                        products.append({
                            "product_id": product_id,
                            "qte": qte_c,  # commandée
                            "qteUG": qte_ug,
                            "qteA": qte_c,  # champ supprimé – valeur alignée
                            "qteR": qte_r,  # reçue
                            "qteAReceptionner": 0,  # non fourni dans ce flux
                            "qteEC": qte_ec,
                        })
                    except (ValueError, TypeError, KeyError) as err:
                        logger.warning("Produit ignoré dans commande %s : %s", order_id, err)
                        continue

                preprocessed_data.append({
                    "order_id": order_id,
                    "supplier_id": supplier_id,
                    "supplier_name": supplier_name,
                    "step": step,
                    "sent_date": sent_date,
                    "delivery_date": delivery_date,
                    "products": products,
                })
            except (ValueError, TypeError) as err:
                logger.warning("Commande ignorée (%s) : %s", achat.get("id", "?"), err)
                continue

        # ------------------------------------------------------------------
        # 2. Fournisseurs ----------------------------------------------------
        # ------------------------------------------------------------------
    supplier_data: List[Dict[str, Any]] = []
    seen_suppliers: Set[tuple[int, str]] = set()

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

    # ------------------------------------------------------------------
    # 3. Produits internes ----------------------------------------------
    # ------------------------------------------------------------------
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
    internal_product_data.extend({
                                     "internal_id": pid,
                                     "pharmacy_id": pharmacy.id,
                                     "name": "empty",
                                 } for pid in new_ids)

    products = cast(List[InternalProduct], common.bulk_process(
        model=InternalProduct,
        data=internal_product_data,
        unique_fields=["pharmacy_id", "internal_id"],
        update_fields=["name"],
    ))
    products_map = {p.internal_id: p for p in products}
    # ------------------------------------------------------------------
    # 4. Commandes -------------------------------------------------------
    # ------------------------------------------------------------------
    order_rows = [{
        "internal_id": obj["order_id"],
        "pharmacy_id": pharmacy.id,
        "supplier_id": suppliers_map[obj["supplier_id"]].id,
        "step": obj["step"],
        "sent_date": obj["sent_date"],
        "delivery_date": obj["delivery_date"],
    } for obj in preprocessed_data]

    orders = common.bulk_process(
        model=Order,
        data=order_rows,
        unique_fields=["internal_id", "pharmacy_id"],
        update_fields=["supplier_id", "step", "sent_date", "delivery_date"],
    )
    orders_map = {o.internal_id: o for o in orders}

    # ------------------------------------------------------------------
    # 5. Liaisons produit‑commande --------------------------------------
    # ------------------------------------------------------------------
    product_order_rows: List[Dict[str, Any]] = []

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
                "qte_r": line["qteR"],
                "qte_a": line["qteA"],
                "qte_ug": line["qteUG"],
                "qte_ec": line["qteEC"],
                "qte_ar": line["qteAReceptionner"],
            })

    common.bulk_process(
        model=ProductOrder,
        data=product_order_rows,
        unique_fields=["order", "product"],
        update_fields=["qte", "qte_r", "qte_a", "qte_ug", "qte_ec", "qte_ar"],
    )

    # ------------------------------------------------------------------
    # 6. Résumé ----------------------------------------------------------
    # ------------------------------------------------------------------
    return {
        "suppliers": suppliers,
        "products": products,
        "orders": orders,
        "product_orders": product_order_rows,
    }


def process_sales(pharmacy, data):
    """
    Process sales records for a pharmacy and its products.

    Args:
        pharmacy: Pharmacy instance associated with the data.
        data: List of dictionaries representing sales data.

    Returns:
        None
    """
    # ------------------------------------------------------------------
    # 1. Pré‑traitement --------------------------------------------------
    # ------------------------------------------------------------------
    preprocessed: List[Dict[str, Any]] = []

    for block in data:
        for vente in block.get("ventes", []):
            heure = vente.get("heure")
            for line in vente.get("lignes", []):
                try:
                    prod_id = int(line.get("prodId"))
                    if prod_id < 0:
                        continue
                    preprocessed.append(
                        {
                            "product_id": str(prod_id),
                            "date": heure,
                            "qte": int(line.get("qte", 0)),
                        }
                    )
                except (ValueError, TypeError, KeyError) as err:
                    logger.warning(
                        "Error preprocessing sale record %s: %s",
                        line.get("prodId", "unknown"),
                        err,
                    )
                    continue

    if not preprocessed:
        return  # rien à importer

    # ------------------------------------------------------------------
    # 2. Récupération des snapshots -------------------------------------
    # ------------------------------------------------------------------
    product_ids = {obj["product_id"] for obj in preprocessed}

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

    # ------------------------------------------------------------------
    # 3. Agrégation des quantités ---------------------------------------
    # ------------------------------------------------------------------
    aggregated_qte: Dict[str, int] = {}
    first_date: Dict[str, str] = {}

    for obj in preprocessed:
        pid = obj["product_id"]
        aggregated_qte[pid] = aggregated_qte.get(pid, 0) + obj["qte"]
        first_date.setdefault(pid, obj["date"])

    # ------------------------------------------------------------------
    # 4. Construction des lignes Sales ----------------------------------
    # ------------------------------------------------------------------
    sales_rows: List[Dict[str, Any]] = []
    for pid, qty in aggregated_qte.items():
        snapshot_id = snapshot_map.get(pid)
        if not snapshot_id:
            continue  # produit sans snapshot : ignoré
        sales_rows.append(
            {
                "product_id": snapshot_id,
                "quantity": common.clamp(qty, -32768, 32767),
                "date": common.parse_date(first_date[pid], False),
            }
        )

    if not sales_rows:
        return

    # ------------------------------------------------------------------
    # 5. Insertion / mise à jour bulk -----------------------------------
    # ------------------------------------------------------------------
    try:
        common.bulk_process(
            model=Sales,
            data=sales_rows,
            unique_fields=["product_id", "date"],
            update_fields=["quantity"],
        )
    except Exception as err:
        logger.error("Error processing sales: %s", err)
        raise