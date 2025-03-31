#!/usr/bin/env python
import csv, os, re, chardet
import traceback
from datetime import datetime, date
import django
from tqdm import tqdm
from django.forms.models import model_to_dict

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "Phardev.settings")
django.setup()

from django.db import transaction
from django.utils import timezone
from data.models import Pharmacy, InternalProduct, InventorySnapshot, Order, ProductOrder, Supplier, Sales, GlobalProduct

CHUNK_SIZE = 1000
FRENCH_MONTH_MAP = {
    "Janvier": 1, "Février": 2, "Mars": 3, "Avril": 4, "Mai": 5,
    "Juin": 6, "Juillet": 7, "Août": 8, "Septembre": 9, "Octobre": 10,
    "Novembre": 11, "Décembre": 12,
}

def detect_encoding(fp, nb=10000):
    try:
        with open(fp, 'rb') as f:
            return chardet.detect(f.read(nb)).get('encoding', 'utf-8')
    except Exception:
        return 'utf-8'


def normalize_date(ds):
    parts = ds.split('/')
    if len(parts[2]) == 2: parts[2] = '20' + parts[2]
    return '/'.join(parts)


def first_day_of_month(dt): return date(dt.year, dt.month, 1)


def add_months(dt, m):
    month = dt.month - 1 + m
    year = dt.year + month // 12
    return date(year, month % 12 + 1, 1)


def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def import_csv_file(fp):
    fn = os.path.basename(fp)
    ph_name = (fn.replace('export', '')
               .replace('EXPORT', '')
               .replace('Export', '')
               .replace('donnees', '')
               .replace('-', '')
               .replace('.csv', '')
               .strip())
    print(f"Pharmacy: {ph_name}")
    # if ph_name != 'Valinco': return

    pharmacy, _ = Pharmacy.objects.get_or_create(name=ph_name)
    enc = detect_encoding(fp)
    with open(fp, newline='', encoding=enc) as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        title = next(reader)[0]
        m = re.search(r'du\s+(\d{2}/\d{2}/\d{2,4})\s+au\s+(\d{2}/\d{2}/\d{2,4})', title)
        if not m:
            print("Erreur date:", title)
            return
        s_str, e_str = m.groups()
        s_str, e_str = normalize_date(s_str), normalize_date(e_str)
        start_date = datetime.strptime(s_str, "%d/%m/%Y").date()
        end_date = datetime.strptime(e_str, "%d/%m/%Y").date()

        headers = []
        while not headers: headers = next(reader)
        try:
            idx_code = headers.index("Code")
            idx_nom = headers.index("Nom")
            idx_stock = headers.index("Stock")
            idx_tva = headers.index("TVA")
            idx_prixV = headers.index("PrixV TTC")
            idx_prixMP = headers.index("Prix MP") if "Prix MP" in headers else None
        except ValueError as e:
            print("Colonne manquante:", e)
            return

        month_cols = []
        for mname in FRENCH_MONTH_MAP:
            if mname in headers:
                month_cols.append((mname, headers.index(mname)))
        month_cols.sort(key=lambda x: x[1])
        exp_months = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)
        if exp_months != len(month_cols):
            print(f"Avertissement: {len(month_cols)} mois vs attendu {exp_months}")

        internal_products_to_create = []
        internal_products_lookup = {}
        orders_lookup = {}
        orders_to_create = []

        snapshot_merge_dict = {}
        sales_merge_dict = {}
        product_order_merge_dict = {}

        first_day = first_day_of_month(start_date)

        # Pré-calculer, pour chaque colonne de mois, le snap_date et l'order_key associés
        precomputed = [
            (i, col_idx, add_months(first_day, i), (add_months(first_day, i).year, add_months(first_day, i).month))
            for i, (mname, col_idx) in enumerate(month_cols)
        ]

        for row in reader:
            if len(row) < len(headers):
                continue

            code_str = row[idx_code].strip()
            if not code_str:
                continue
            try:
                internal_id = int(code_str.replace(' ', ''))
            except ValueError as e:
                print(e)
                continue

            prod_name = row[idx_nom].strip()
            tva_str = row[idx_tva].strip().replace('"', '').replace(',', '.')
            try:
                tva = float(tva_str) / 100 if tva_str else None
            except ValueError as e:
                print(e)
                tva = None

            # Création ou récupération du produit
            if internal_id not in internal_products_lookup:
                # Vérifier si le code comporte exactement 13 caractères (attention aux éventuels espaces ou zéros manquants)
                if len(code_str) == 13:
                    # Récupérer ou créer le GlobalProduct associé
                    global_product, _ = GlobalProduct.objects.get_or_create(
                        code_13_ref=code_str,
                        defaults={'name': prod_name}
                    )
                    p = InternalProduct(
                        pharmacy=pharmacy,
                        internal_id=internal_id,
                        name=prod_name,
                        TVA=tva,
                        code_13_ref=global_product
                    )
                else:
                    p = InternalProduct(
                        pharmacy=pharmacy,
                        internal_id=internal_id,
                        name=prod_name,
                        TVA=tva
                    )
                internal_products_to_create.append(p)
                internal_products_lookup[internal_id] = p
            else:
                p = internal_products_lookup[internal_id]

            # Pré-calculer les valeurs de prix (elles sont identiques pour tous les mois de cette ligne)
            prixV_val = row[idx_prixV].strip().replace('"', '').replace(',', '.')
            try:
                p_tax_val = float(prixV_val)
            except ValueError:
                p_tax_val = None

            if idx_prixMP is not None:
                prixMP_val = row[idx_prixMP].strip().replace('"', '').replace(',', '.')
                try:
                    p_mp_val = float(prixMP_val)
                except ValueError:
                    p_mp_val = 0
            else:
                p_mp_val = 0

            # Boucle sur les colonnes de mois avec les valeurs pré-calculées
            for i, col_idx, snap_date, order_key in precomputed:
                qty_str = row[col_idx].strip()
                try:
                    qty = int(qty_str)
                except ValueError:
                    qty = 0

                key = (internal_id, snap_date)
                # Fusionner les InventorySnapshot
                if key in snapshot_merge_dict:
                    snapshot_merge_dict[key].stock += qty
                else:
                    snapshot_merge_dict[key] = InventorySnapshot(
                        product=p, date=snap_date, stock=qty,
                        price_with_tax=p_tax_val, weighted_average_price=p_mp_val
                    )
                # Fusionner les ventes (sales)
                sales_merge_dict[key] = sales_merge_dict.get(key, 0) + qty

                # Gestion des Orders, groupés par mois
                if order_key not in orders_lookup:
                    sent_dt = timezone.make_aware(datetime.combine(snap_date, datetime.min.time()))
                    order = Order(
                        internal_id=int(f"{snap_date.year}{snap_date.month:02d}"),
                        pharmacy=pharmacy, step=1,
                        sent_date=sent_dt, delivery_date=sent_dt
                    )
                    orders_lookup[order_key] = order
                    orders_to_create.append(order)
                else:
                    order = orders_lookup[order_key]

                # Fusionner les ProductOrder pour le même order et produit
                po_key = (order_key, internal_id)
                if po_key in product_order_merge_dict:
                    existing_po = product_order_merge_dict[po_key]
                    existing_po.qte += qty
                    existing_po.qte_r += qty
                    existing_po.qte_a += qty
                    existing_po.qte_ar += qty
                else:
                    product_order_merge_dict[po_key] = ProductOrder(
                        order=order, product=p, qte=qty, qte_r=qty, qte_a=qty,
                        qte_ug=0, qte_ec=0, qte_ar=qty
                    )

        # Conversion des dictionnaires en listes une fois le traitement terminé
        inventory_snapshots_to_create = list(snapshot_merge_dict.values())
        sales_temp = [(iid, sdate, qty) for ((iid, sdate), qty) in sales_merge_dict.items()]
        product_orders_to_create = list(product_order_merge_dict.values())

        with transaction.atomic():
            # Insertion des InternalProduct sans return_defaults
            if internal_products_to_create:
                for chunk in chunk_list(internal_products_to_create, CHUNK_SIZE):
                    InternalProduct.objects.bulk_create(chunk)

            inserted_ids = [p.internal_id for p in internal_products_to_create]
            internal_products_lookup = {
                p.internal_id: p
                for p in InternalProduct.objects.filter(pharmacy=pharmacy, internal_id__in=inserted_ids)
            }

            for inv in inventory_snapshots_to_create:
                inv.product = internal_products_lookup[inv.product.internal_id]
            for po in product_orders_to_create:
                po.product = internal_products_lookup[po.product.internal_id]

            if orders_to_create:
                for chunk in chunk_list(orders_to_create, CHUNK_SIZE):
                    Order.objects.bulk_create(chunk)
            # Recharger les Order pour récupérer leurs PK réels
            order_ids = [o.internal_id for o in orders_to_create]
            orders_lookup = {
                o.internal_id: o
                for o in Order.objects.filter(pharmacy=pharmacy, internal_id__in=order_ids)
            }

            # Mise à jour de la référence Order dans ProductOrder
            for po in product_orders_to_create:
                po.order = orders_lookup[po.order.internal_id]

            # Insertion des InventorySnapshot et ProductOrder
            if inventory_snapshots_to_create:
                for chunk in chunk_list(inventory_snapshots_to_create, CHUNK_SIZE):
                    try:
                        InventorySnapshot.objects.bulk_create(chunk)
                    except:
                        print(traceback.format_exc())
                        for instance in chunk:
                            print(model_to_dict(instance))
                            print(internal_products_lookup[instance.product.internal_id])
                        exit()
            if product_orders_to_create:
                for chunk in chunk_list(product_orders_to_create, CHUNK_SIZE):
                    ProductOrder.objects.bulk_create(chunk)

        print('PAAAAAAAAAAAAAAAAAAAAA')
        # Calculer une fois le premier jour du mois
        start_month = first_day_of_month(start_date)

        # Filtrer et créer un dictionnaire des InventorySnapshot existants
        inv_snaps_db = InventorySnapshot.objects.filter(
            product__pharmacy=pharmacy,
            date__gte=start_month,
            date__lte=end_date
        ).select_related('product')
        inv_snap_dict = {(snap.product.internal_id, snap.date): snap for snap in inv_snaps_db}
        available_keys = set(inv_snap_dict.keys())

        # Générer la liste des Sales à créer en fusionnant les quantités
        sales_to_create = [
            Sales(product=inv_snap_dict[(iid, sdate)], date=sdate, quantity=qty)
            for iid, sdate, qty in sales_temp if (iid, sdate) in available_keys
        ]

        # Insertion en base en utilisant bulk_create avec batch_size pour découper automatiquement
        if sales_to_create:
            Sales.objects.bulk_create(sales_to_create, batch_size=CHUNK_SIZE)

        print(f"{fn} importé avec succès.")

import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import shutil


def import_csv_batch(d='exports'):
    # Calculer le répertoire parent du dossier source
    parent_dir = os.path.dirname(os.path.abspath(d))
    # Construire le chemin vers le dossier "processed" qui est au même niveau que le dossier source
    processed_dir = os.path.join(parent_dir, "processed")
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)

    # Récupérer tous les fichiers CSV dans le dossier source
    files = [os.path.join(d, fn) for fn in os.listdir(d)
             if os.path.isfile(os.path.join(d, fn)) and fn.endswith(".csv")]

    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(import_csv_file, fp): fp for fp in files}
        for future in tqdm(as_completed(futures), total=len(futures)):
            fp = futures[future]
            try:
                future.result()  # Traitement réussi
                dest = os.path.join(processed_dir, os.path.basename(fp))
                shutil.move(fp, dest)
                print(f"Fichier {fp} déplacé vers {dest}")
            except Exception as e:
                print(f"Erreur lors du traitement de {fp} : {e}")




if __name__ == "__main__":
    import_csv_batch()
