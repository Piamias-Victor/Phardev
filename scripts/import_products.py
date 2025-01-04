import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'Phardev.settings')
django.setup()

import re
import sys
from decimal import Decimal, InvalidOperation

import pandas as pd
from tqdm import tqdm

from data.models import GlobalProduct

# Configure Django settings


def extract_year(value):
    """
    Extracts the first occurrence of digits in a string and returns it as an integer.
    Returns None if no digits are found.
    """
    match = re.search(r'\d+', str(value))
    if match:
        return int(match.group())
    return None


def clean_decimal(value):
    """
    Cleans a string by removing '%' and non-numeric characters, then converts it to Decimal.
    Returns None if conversion fails.
    """
    if pd.isna(value):
        return None
    try:
        # Remove '%' and any non-digit characters except '.' and '-'
        value = re.sub(r'[^\d\.-]', '', str(value)).strip()
        return Decimal(value)
    except (InvalidOperation, ValueError):
        return None


def clean_code_13_ref(code_13):
    """
    Cleans the 'code_13_ref' by removing non-digit characters and ensuring it's 13 digits.
    """
    if pd.isna(code_13):
        return None
    code_13 = str(code_13).replace(' ', '').strip()  # Remove non-breaking spaces and strip
    # Remove any other non-digit characters
    code_13 = re.sub(r'\D', '', code_13)
    return code_13 if len(code_13) == 13 else code_13[:13]  # Ensure it's at most 13 digits



def import_data_in_batches(df, batch_size=10000):
    """
    Imports data from a pandas DataFrame into the GlobalProduct model in batches.
    """
    # Pre-clean the DataFrame to optimize row processing
    df['code_13_ref'] = df["GENCOD13=EAN13"].apply(clean_code_13_ref)
    df['year'] = df["DATE"].apply(extract_year)
    df['tva_percentage'] = df["% TVA"].apply(clean_decimal)
    df['free_access'] = df["LIBRE ACCES"].apply(lambda x: bool(x) if not pd.isna(x) else None)

    # Replace NaNs with None for nullable fields
    nullable_fields = ["MARQUE - LABO", "LABORATOIRE - DISTRIBUTEUR", "GAMME",
                       "SPECIFICITE", "FAMILLE", "SOUS FAMILLE"]
    for field in nullable_fields:
        df[field] = df[field].where(~df[field].isna(), None)

    # Optionally, clean the 'PRODUIT' column by stripping whitespace
    df['PRODUIT'] = df['PRODUIT'].astype(str).str.strip()

    # Initialize batch list
    model_instances = []

    # Iterate using itertuples for better performance
    for row in tqdm(df.itertuples(index=False), total=df.shape[0], desc="Importing"):
        instance = GlobalProduct(
            code_13_ref=row.code_13_ref,
            name=row.PRODUIT,
            year=None if pd.isna(row.year) else row.year,
            universe=row.UNIVERS,
            category=row.CATÉGORIE,
            sub_category=row._asdict().get("SOUS CATÉGORIE"),
            brand_lab=row._asdict().get("MARQUE_-_LABO"),
            lab_distributor=row._asdict().get("LABORATOIRE_-_DISTRIBUTEUR"),
            range_name=row.GAMME,
            specificity=row.SPECIFICITE,
            family=row.FAMILLE,
            sub_family=row._asdict().get('SOUS_FAMILLE'),
            tva_percentage=row.tva_percentage,
            free_access=row.free_access,
        )
        model_instances.append(instance)

        # Insert in batches
        if len(model_instances) >= batch_size:
            GlobalProduct.objects.bulk_create(model_instances,
            ignore_conflicts = True  # Ignore les conflits liés aux clés uniques (code_13_ref)
            )
            model_instances = []

    # Insert any remaining instances
    if model_instances:
        GlobalProduct.objects.bulk_create(model_instances,
                                          ignore_conflicts=True
                                          # Ignore les conflits liés aux clés uniques (code_13_ref)
                                          )

    print("Importation terminée avec succès.")


def main():
    # Path to the Excel file
    excel_file = "CODES PRODUITS OFFICIELS SANS NUMEROTATION 2.xlsx"

    # Read the Excel file
    try:
        df = pd.read_excel(excel_file)
    except FileNotFoundError:
        print(f"Fichier {excel_file} non trouvé.")
        sys.exit(1)
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier Excel: {e}")
        sys.exit(1)

    # Optionnel : Supprimer les objets existants
    # Décommentez la ligne suivante si vous souhaitez supprimer tous les enregistrements existants
    GlobalProduct.objects.all().delete()

    # Import data
    import_data_in_batches(df, batch_size=10000)


if __name__ == '__main__':
    main()
