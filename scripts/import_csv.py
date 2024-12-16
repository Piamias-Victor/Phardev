import os
import django
import pandas as pd
import re
from tqdm import tqdm
from decimal import Decimal, InvalidOperation

os.environ['DJANGO_SETTINGS_MODULE'] = 'Phardev.settings'
django.setup()
from data.models import GlobalProduct

df = pd.read_excel("CODES PRODUITS OFFICIELS SANS NUMEROTATION 2.xlsx")


def extract_year(value):
    # Extraire les chiffres uniquement
    match = re.search(r'\d+', str(value))
    if match:
        return int(match.group(0))  # Convertir en entier
    return None  # Retourne None si aucun chiffre n'est trouvé


def clean_decimal(value):
    try:
        # Supprime les espaces ou les caractères spéciaux
        value = str(value).replace('%', '').strip()
        # Convertit en Decimal
        return Decimal(value)
    except (InvalidOperation, ValueError):
        return None  # Retourne None si la conversion échoue

import pandas as pd
from tqdm import tqdm
GlobalProduct.objects.all().delete()
def import_data_in_batches(df, batch_size=10000):
    model_instances = []
    for _, row in tqdm(df.iterrows(), total=df.shape[0]):
        code_13 = row["GENCOD13=EAN13"]
        if len(str(code_13)) > 13:
            code_13 = code_13.replace(' ', '')
        # Création d'une instance pour chaque ligne
        instance = GlobalProduct(
            code_13_ref=code_13,  # Colonne Excel correspondante
            name=row["PRODUIT"],
            year=extract_year(row["DATE"]),  # Nettoyage des lettres avant insertion

            universe=row["UNIVERS"],
            category=row["CATÉGORIE"],
            sub_category=row["SOUS CATÉGORIE"],

            brand_lab=row["MARQUE - LABO"] if not pd.isna(row["MARQUE - LABO"]) else None,
            lab_distributor=row["LABORATOIRE - DISTRIBUTEUR"] if not pd.isna(row["LABORATOIRE - DISTRIBUTEUR"]) else None,
            range_name=row["GAMME"] if not pd.isna(row["GAMME"]) else None,
            specificity=row["SPECIFICITE"] if not pd.isna(row["SPECIFICITE"]) else None,
            family=row["FAMILLE"] if not pd.isna(row["FAMILLE"]) else None,
            sub_family=row["SOUS FAMILLE"] if not pd.isna(row["SOUS FAMILLE"]) else None,

            tva_percentage=clean_decimal(row["% TVA"]),
            free_access=bool(row["LIBRE ACCES"]) if not pd.isna(row["LIBRE ACCES"]) else False,
        )
        model_instances.append(instance)

        # Vérifier si on a atteint le batch_size
        if len(model_instances) == batch_size:
            # Insérer les instances en base
            GlobalProduct.objects.bulk_create(
                model_instances,
                ignore_conflicts=True  # Ignore les conflits liés aux clés uniques
            )
            model_instances = []  # Réinitialiser la liste pour le prochain lot

    # Insérer les dernières instances restantes
    if model_instances:
        GlobalProduct.objects.bulk_create(
            model_instances,
            ignore_conflicts=True  # Ignore les conflits liés aux clés uniques
        )

    print("Importation terminée avec succès.")


import_data_in_batches(df, batch_size=1000)
