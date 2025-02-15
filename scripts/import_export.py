#!/usr/bin/env python
import csv, os, re, sys, chardet
from datetime import datetime, date
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "Phardev.settings")
django.setup()

from django.db import transaction
from data.models import (
    Pharmacy, InternalProduct, InventorySnapshot,
    Order, ProductOrder, Supplier, Sales
)

FRENCH_MONTH_MAP = {
    "Janvier": 1, "Février": 2, "Mars": 3, "Avril": 4, "Mai": 5,
    "Juin": 6, "Juillet": 7, "Août": 8, "Septembre": 9, "Octobre": 10,
    "Novembre": 11, "Décembre": 12,
}


def detect_encoding(filepath, num_bytes=10000):
    with open(filepath, 'rb') as f:
        return chardet.detect(f.read(num_bytes)).get('encoding', 'utf-8')


def normalize_date(ds):
    # Convertit une date au format dd/mm/yy ou dd/mm/yyyy en dd/mm/yyyy
    parts = ds.split('/')
    if len(parts[2]) == 2:
        parts[2] = '20' + parts[2]
    return '/'.join(parts)


def first_day_of_month(dt):
    """Renvoie le premier jour du mois de la date dt."""
    return date(dt.year, dt.month, 1)


def add_months(dt, months):
    """Ajoute un nombre de mois à une date et retourne le premier jour du mois résultant."""
    month = dt.month - 1 + months
    year = dt.year + month // 12
    month = month % 12 + 1
    return date(year, month, 1)


def import_csv_file(filepath):
    filename = os.path.basename(filepath)
    pharmacy_name = filename.replace('export', '').replace('EXPORT', '').replace('Export', '') \
        .replace('donnees', '').replace('-', '').replace('.csv', '').strip()
    print(f"Pharmacy extraite : {pharmacy_name}")
    pharmacy, _ = Pharmacy.objects.get_or_create(name=pharmacy_name)
    encoding = detect_encoding(filepath)

    with open(filepath, newline='', encoding=encoding) as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        title = next(reader)[0]
        # Extraction de la plage de dates depuis le titre (années sur 2 ou 4 chiffres)
        match = re.search(r'du\s+(\d{2}/\d{2}/\d{2,4})\s+au\s+(\d{2}/\d{2}/\d{2,4})', title)
        if not match:
            print(f"Impossible d'extraire la plage de dates depuis le titre : {title}")
            return
        start_date_str, end_date_str = match.groups()
        start_date_str = normalize_date(start_date_str)
        end_date_str = normalize_date(end_date_str)
        start_date = datetime.strptime(start_date_str, "%d/%m/%Y").date()
        end_date = datetime.strptime(end_date_str, "%d/%m/%Y").date()

        headers = []
        while not headers:
            headers = next(reader)
            if headers:
                break
        try:
            idx_code = headers.index("Code")
            idx_nom = headers.index("Nom")
            idx_stock = headers.index("Stock")
            idx_tva = headers.index("TVA")
            idx_prixV = headers.index("PrixV TTC")
            if "Prix MP" in headers:
                idx_prixMP = headers.index("Prix MP")
            else:
                idx_prixMP = None
        except ValueError as e:
            print(f"Colonne manquante dans le CSV : {e}")
            return

        # Identification des colonnes correspondant aux mois
        month_columns = []
        for month in FRENCH_MONTH_MAP.keys():
            if month in headers:
                month_columns.append((month, headers.index(month)))
        # Trier par ordre d'apparition dans le CSV
        month_columns.sort(key=lambda x: x[1])

        # Calcul du nombre de mois attendu (on ajoute 1 pour inclure le mois de début)
        expected_months = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)
        if expected_months != len(month_columns):
            print(f"Avertissement : le nombre de colonnes de mois ({len(month_columns)}) "
                  f"ne correspond pas à la plage de dates attendue ({expected_months}).")

        orders_by_month = {}  # clé : (année, mois) -> Order
        with transaction.atomic():
            for row in reader:
                # Vérifier que la ligne comporte bien toutes les colonnes
                if len(row) < len(headers):
                    continue

                # Création ou mise à jour de l'InternalProduct
                code_str = row[idx_code].strip()
                if not code_str:
                    continue
                try:
                    internal_id = int(code_str)
                except ValueError:
                    continue

                product_name = row[idx_nom].strip()
                tva_str = row[idx_tva].strip().replace('"', '')
                try:
                    # La TVA dans le CSV est en pourcentage (ex: "2.1")
                    tva_decimal = float(tva_str) / 100 if tva_str else None
                except ValueError:
                    tva_decimal = None

                product, _ = InternalProduct.objects.update_or_create(
                    pharmacy=pharmacy,
                    internal_id=internal_id,
                    defaults={
                        'name': product_name,
                        'TVA': tva_decimal,
                    }
                )

                # Pour chaque colonne correspondant à un mois, créer un snapshot et un sales
                for i, (month_name, col_idx) in enumerate(month_columns):
                    qty_str = row[col_idx].strip()
                    if not qty_str:
                        continue
                    try:
                        qty = int(qty_str)
                    except ValueError:
                        qty = 0

                    # Calcul de la date du snapshot à partir de start_date + i mois
                    snapshot_date = add_months(first_day_of_month(start_date), i)

                    # Extraction du prix (conversion de la virgule en point)
                    prixV_clean = row[idx_prixV].strip().replace('"', '').replace(',', '.')
                    try:
                        prix_with_tax = float(prixV_clean)
                    except ValueError:
                        prix_with_tax = None

                    # Extraction de "Prix MP" si présent, sinon on met 0
                    if idx_prixMP is not None:
                        prixMP_clean = row[idx_prixMP].strip().replace('"', '').replace(',', '.')
                        try:
                            prix_mp = float(prixMP_clean)
                        except ValueError:
                            prix_mp = 0
                    else:
                        prix_mp = 0

                    snapshot, _ = InventorySnapshot.objects.update_or_create(
                        product=product,
                        date=snapshot_date,
                        defaults={
                            'stock': qty,
                            'price_with_tax': prix_with_tax,
                            'weighted_average_price': prix_mp,
                        }
                    )
                    from django.utils import timezone
                    sent_datetime = timezone.make_aware(datetime.combine(snapshot_date, datetime.min.time()))

                    # Création d'une commande fictive pour ce mois si elle n'existe pas déjà
                    order_key = (snapshot_date.year, snapshot_date.month)
                    if order_key not in orders_by_month:
                        fake_order = Order.objects.create(
                            internal_id=int(f"{snapshot_date.year}{snapshot_date.month:02d}"),
                            pharmacy=pharmacy,
                            step=1,  # étape initiale
                            sent_date=sent_datetime,
                            delivery_date=snapshot_date,
                        )
                        orders_by_month[order_key] = fake_order
                    else:
                        fake_order = orders_by_month[order_key]

                    # Création ou mise à jour du ProductOrder pour ce produit dans cette commande
                    ProductOrder.objects.update_or_create(
                        order=fake_order,
                        product=product,
                        defaults={
                            'qte': qty,
                            'qte_r': qty,
                            'qte_a': qty,
                            'qte_ug': 0,
                            'qte_ec': 0,
                            'qte_ar': qty,
                        }
                    )

                    # Création ou mise à jour d'un Sales pour ce snapshot (donc pour ce produit et ce mois)
                    Sales.objects.update_or_create(
                        product=snapshot,  # La clé étrangère pointe vers l'InventorySnapshot
                        date=snapshot_date,
                        defaults={'quantity': qty}
                    )

    print(f"{filename} importé avec succès.")


for filename in os.listdir('exports'):
    filepath = os.path.join('exports', filename)
    if os.path.isfile(filepath) and filename.endswith(".csv"):
        import_csv_file(filepath)
