import os
import pandas as pd
from tqdm import tqdm
from decimal import Decimal, InvalidOperation
import django
import re
from data.models import GlobalProduct

# Configuration Django
os.environ['DJANGO_SETTINGS_MODULE'] = 'Phardev.settings'
django.setup()

# Fonction pour extraire l'année
def extract_year(value):
    match = re.search(r'\d+', str(value))
    if match:
        return int(match.group(0))  # Convertir en entier
    return None  # Retourne None si aucun chiffre n'est trouvé

# Fonction pour nettoyer les décimaux
def clean_decimal(value):
    try:
        value = str(value).replace('%', '').strip()
        return Decimal(value)
    except (InvalidOperation, ValueError):
        return None

# Fonction pour importer les données dans des lots
def import_data_in_batches(df, batch_size=10000):
    model_instances = []
    for _, row in tqdm(df.iterrows(), total=df.shape[0]):
        # Extraire et nettoyer les données de chaque ligne
        code_13 = row["GENCOD13=EAN13"]
        if len(str(code_13)) > 13:
            code_13 = code_13.replace(' ', '')  # Nettoyer les espaces si nécessaire

        # Créer une instance de GlobalProduct
        instance = GlobalProduct(
            code_13_ref=code_13,  # Colonne Excel correspondante
            name=row["PRODUIT"],
            year=extract_year(row["DATE"]),  # Nettoyer et extraire l'année
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

        # Insérer les instances en base par lot
        if len(model_instances) == batch_size:
            GlobalProduct.objects.bulk_create(
                model_instances,
                ignore_conflicts=True
            )
            model_instances = []

    # Insérer les dernières instances restantes
    if model_instances:
        GlobalProduct.objects.bulk_create(
            model_instances,
            ignore_conflicts=True
        )

    print("Importation terminée avec succès.")


def process_csv_files_in_directory(directory_path):
    # Parcourir chaque fichier dans le dossier
    for file_name in os.listdir(directory_path):
        if file_name.endswith(".csv"):
            file_path = os.path.join(directory_path, file_name)
            print(f"Traitement du fichier : {file_name}")

            # Lire le fichier CSV
            df = pd.read_csv(file_path)

            # Importer les données dans la base de données
            import_data_in_batches(df, batch_size=1000)

# Exemple d'appel de la fonction pour traiter tous les fichiers CSV dans un dossier
directory_path = "/exports"  #
process_csv_files_in_directory(directory_path)
