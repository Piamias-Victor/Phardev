import os
import sys
import re
import django
import pandas as pd
from tqdm import tqdm
from decimal import Decimal, InvalidOperation
from django.db import transaction

# 1. Configuration Django
# -----------------------------------------------------------------------------
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'Phardev.settings')
django.setup()

# 2. Imports Django
# -----------------------------------------------------------------------------
from data.models import GlobalProduct


# 3. Fonctions utilitaires pour le nettoyage des données
# -----------------------------------------------------------------------------
def extract_year(value):
    """
    Tente d'extraire une année valide (entre 1900 et 2100) à partir d'une valeur donnée.
    Retourne l'année en tant qu'entier ou None si la conversion échoue.
    """
    try:
        if pd.isna(value):
            return None
        value_str = str(value).strip()
        digits = ''.join(filter(str.isdigit, value_str))
        if digits:
            year = int(digits)
            if 1900 <= year <= 2100:
                return year
        return None
    except Exception:
        return None


def clean_code_13_ref(code_13):
    """
    Nettoie la référence EAN13.
    - Supprime les espaces et caractères non numériques.
    - Tronque à 13 caractères pour éviter les dépassements.
    Retourne None si la valeur initiale est NaN.
    """
    if pd.isna(code_13):
        return None
    # Supprime les espaces et caractères non numériques
    code_13 = str(code_13).replace(' ', '').strip()
    code_13 = re.sub(r'\D', '', code_13)
    # Tronque à 13 caractères maximum
    return code_13[:13]


def clean_decimal(value):
    """
    Nettoie et convertit une valeur en Decimal.
    Retourne None si la conversion échoue ou si la valeur est vide/NaN.
    """
    try:
        if pd.isna(value) or value == '':
            return None
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError, TypeError):
        return None


# 4. Fonction principale d'import
# -----------------------------------------------------------------------------
def main():
    """
    Lit un fichier Excel, nettoie les données et les insère (ou met à jour)
    dans la base via le modèle GlobalProduct, par lots (batch).
    """
    # 4.1. Lecture du fichier Excel
    excel_file = "CODES PRODUITS OFFICIELS SANS NUMEROTATION 2.xlsx"

    try:
        df = pd.read_excel(excel_file)
    except FileNotFoundError:
        print(f"Fichier {excel_file} non trouvé.")
        sys.exit(1)
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier Excel: {e}")
        sys.exit(1)

    # 4.2. Renommage des colonnes pour simplifier l'accès
    df.rename(
        columns={
            "GENCOD13=EAN13": "code_13_ref_raw",
            "DATE": "date",
            "UNIVERS": "universe",
            "CATÉGORIE": "category",
            "SOUS CATÉGORIE": "sub_category",
            "MARQUE - LABO": "brand_lab",
            "LABORATOIRE - DISTRIBUTEUR": "lab_distributor",
            "GAMME": "range_name",
            "SPECIFICITE": "specificity",
            "FAMILLE": "family",
            "SOUS FAMILLE": "sub_family",
            "% TVA": "tva_percentage",
            "LIBRE ACCES": "free_access",
            "PRODUIT": "product_name",
        },
        inplace=True
    )

    # 4.3. Nettoyage et prétraitement du DataFrame
    # -------------------------------------------------------------------------
    # Nettoyage du code EAN13
    df["code_13_ref"] = df["code_13_ref_raw"].apply(clean_code_13_ref)
    df.drop(columns=["code_13_ref_raw"], inplace=True)

    # Nettoyage des pourcentages TVA
    df["tva_percentage"] = df["tva_percentage"].apply(clean_decimal)

    # Remplacer les NaNs par None pour certains champs nullable
    nullable_fields = ["brand_lab", "lab_distributor", "range_name",
                       "specificity", "family", "sub_family"]
    df[nullable_fields] = df[nullable_fields].where(~df[nullable_fields].isna(), None)

    # Nettoyage de la colonne 'product_name'
    df["product_name"] = df["product_name"].astype(str).str.strip()

    # Extraction de l'année (champ year)
    df["year"] = df["date"].apply(extract_year)
    df["year"] = df["year"].astype("Int64")  # Gère les valeurs nulles

    # 4.4. Traitement par lots (batch)
    # -------------------------------------------------------------------------
    batch_size = 1000
    total_rows = df.shape[0]

    for start in tqdm(range(0, total_rows, batch_size), desc="Processing Batches"):
        end = min(start + batch_size, total_rows)
        batch_df = df.iloc[start:end]

        # Récupérer la liste des codes EAN13 dans ce batch
        batch_codes = batch_df["code_13_ref"].tolist()

        # Sélectionner les GlobalProduct existants correspondant aux codes
        existing_products = GlobalProduct.objects.filter(code_13_ref__in=batch_codes)
        existing_products_dict = {prod.code_13_ref: prod for prod in existing_products}

        # Listes pour la création et la mise à jour
        products_to_create = []
        products_to_update = []

        # 4.4.1. Préparation des objets à créer ou mettre à jour
        # ---------------------------------------------------------------------
        for row in batch_df.itertuples(index=False):
            product_data = {
                "name": row.product_name,
                "year": row.year if not pd.isna(row.year) else None,
                "universe": row.universe,
                "category": row.category,
                "sub_category": row.sub_category,
                "brand_lab": row.brand_lab,
                "lab_distributor": row.lab_distributor,
                "range_name": row.range_name,
                "specificity": row.specificity,
                "family": row.family,
                "sub_family": row.sub_family,
                "tva_percentage": Decimal(row.tva_percentage) if pd.notna(row.tva_percentage) else None,
                "free_access": bool(row.free_access) if pd.notna(row.free_access) else False,
            }

            code = row.code_13_ref
            if code in existing_products_dict:
                # Mise à jour
                obj = existing_products_dict[code]
                for key, value in product_data.items():
                    setattr(obj, key, value)
                products_to_update.append(obj)
            else:
                # Création
                new_product = GlobalProduct(code_13_ref=code, **product_data)
                products_to_create.append(new_product)

        # 4.4.2. Exécution des opérations en base
        # ---------------------------------------------------------------------
        with transaction.atomic():
            if products_to_create:
                GlobalProduct.objects.bulk_create(products_to_create, ignore_conflicts=True)
            if products_to_update:
                GlobalProduct.objects.bulk_update(
                    products_to_update,
                    fields=[
                        "name", "year", "universe", "category", "sub_category",
                        "brand_lab", "lab_distributor", "range_name", "specificity",
                        "family", "sub_family", "tva_percentage", "free_access"
                    ]
                )

        # 4.4.3. Journalisation du batch traité
        # ---------------------------------------------------------------------
        print(f"Batch {start // batch_size + 1} processed: {end - start} records.")


# 5. Point d'entrée du script
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    main()
import os
import sys
import re
import django
import pandas as pd
from tqdm import tqdm
from decimal import Decimal, InvalidOperation
from django.db import transaction

# 1. Configuration Django
# -----------------------------------------------------------------------------
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'Phardev.settings')
django.setup()

# 2. Imports Django
# -----------------------------------------------------------------------------
from data.models import GlobalProduct


# 3. Fonctions utilitaires pour le nettoyage des données
# -----------------------------------------------------------------------------
def extract_year(value):
    """
    Tente d'extraire une année valide (entre 1900 et 2100) à partir d'une valeur donnée.
    Retourne l'année en tant qu'entier ou None si la conversion échoue.
    """
    try:
        if pd.isna(value):
            return None
        value_str = str(value).strip()
        digits = ''.join(filter(str.isdigit, value_str))
        if digits:
            year = int(digits)
            if 1900 <= year <= 2100:
                return year
        return None
    except Exception:
        return None


def clean_code_13_ref(code_13):
    """
    Nettoie la référence EAN13.
    - Supprime les espaces et caractères non numériques.
    - Tronque à 13 caractères pour éviter les dépassements.
    Retourne None si la valeur initiale est NaN.
    """
    if pd.isna(code_13):
        return None
    # Supprime les espaces et caractères non numériques
    code_13 = str(code_13).replace(' ', '').strip()
    code_13 = re.sub(r'\D', '', code_13)
    # Tronque à 13 caractères maximum
    return code_13[:13]


def clean_decimal(value):
    """
    Nettoie et convertit une valeur en Decimal.
    Retourne None si la conversion échoue ou si la valeur est vide/NaN.
    """
    try:
        if pd.isna(value) or value == '':
            return None
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError, TypeError):
        return None


# 4. Fonction principale d'import
# -----------------------------------------------------------------------------
def main():
    """
    Lit un fichier Excel, nettoie les données et les insère (ou met à jour)
    dans la base via le modèle GlobalProduct, par lots (batch).
    """
    # 4.1. Lecture du fichier Excel
    excel_file = "CODES PRODUITS OFFICIELS SANS NUMEROTATION 2.xlsx"

    try:
        df = pd.read_excel(excel_file)
    except FileNotFoundError:
        print(f"Fichier {excel_file} non trouvé.")
        sys.exit(1)
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier Excel: {e}")
        sys.exit(1)

    # 4.2. Renommage des colonnes pour simplifier l'accès
    df.rename(
        columns={
            "GENCOD13=EAN13": "code_13_ref_raw",
            "DATE": "date",
            "UNIVERS": "universe",
            "CATÉGORIE": "category",
            "SOUS CATÉGORIE": "sub_category",
            "MARQUE - LABO": "brand_lab",
            "LABORATOIRE - DISTRIBUTEUR": "lab_distributor",
            "GAMME": "range_name",
            "SPECIFICITE": "specificity",
            "FAMILLE": "family",
            "SOUS FAMILLE": "sub_family",
            "% TVA": "tva_percentage",
            "LIBRE ACCES": "free_access",
            "PRODUIT": "product_name",
        },
        inplace=True
    )

    # 4.3. Nettoyage et prétraitement du DataFrame
    # -------------------------------------------------------------------------
    # Nettoyage du code EAN13
    df["code_13_ref"] = df["code_13_ref_raw"].apply(clean_code_13_ref)
    df.drop(columns=["code_13_ref_raw"], inplace=True)

    # Nettoyage des pourcentages TVA
    df["tva_percentage"] = df["tva_percentage"].apply(clean_decimal)

    # Remplacer les NaNs par None pour certains champs nullable
    nullable_fields = ["brand_lab", "lab_distributor", "range_name",
                       "specificity", "family", "sub_family"]
    df[nullable_fields] = df[nullable_fields].where(~df[nullable_fields].isna(), None)

    # Nettoyage de la colonne 'product_name'
    df["product_name"] = df["product_name"].astype(str).str.strip()

    # Extraction de l'année (champ year)
    df["year"] = df["date"].apply(extract_year)
    df["year"] = df["year"].astype("Int64")  # Gère les valeurs nulles

    # 4.4. Traitement par lots (batch)
    # -------------------------------------------------------------------------
    batch_size = 1000
    total_rows = df.shape[0]

    for start in tqdm(range(0, total_rows, batch_size), desc="Processing Batches"):
        end = min(start + batch_size, total_rows)
        batch_df = df.iloc[start:end]

        # Récupérer la liste des codes EAN13 dans ce batch
        batch_codes = batch_df["code_13_ref"].tolist()

        # Sélectionner les GlobalProduct existants correspondant aux codes
        existing_products = GlobalProduct.objects.filter(code_13_ref__in=batch_codes)
        existing_products_dict = {prod.code_13_ref: prod for prod in existing_products}

        # Listes pour la création et la mise à jour
        products_to_create = []
        products_to_update = []

        # 4.4.1. Préparation des objets à créer ou mettre à jour
        # ---------------------------------------------------------------------
        for row in batch_df.itertuples(index=False):
            product_data = {
                "name": row.product_name,
                "year": row.year if not pd.isna(row.year) else None,
                "universe": row.universe,
                "category": row.category,
                "sub_category": row.sub_category,
                "brand_lab": row.brand_lab,
                "lab_distributor": row.lab_distributor,
                "range_name": row.range_name,
                "specificity": row.specificity,
                "family": row.family,
                "sub_family": row.sub_family,
                "tva_percentage": Decimal(row.tva_percentage) if pd.notna(row.tva_percentage) else None,
                "free_access": bool(row.free_access) if pd.notna(row.free_access) else False,
            }

            code = row.code_13_ref
            if code in existing_products_dict:
                # Mise à jour
                obj = existing_products_dict[code]
                for key, value in product_data.items():
                    setattr(obj, key, value)
                products_to_update.append(obj)
            else:
                # Création
                new_product = GlobalProduct(code_13_ref=code, **product_data)
                products_to_create.append(new_product)

        # 4.4.2. Exécution des opérations en base
        # ---------------------------------------------------------------------
        with transaction.atomic():
            if products_to_create:
                GlobalProduct.objects.bulk_create(products_to_create, ignore_conflicts=True)
            if products_to_update:
                GlobalProduct.objects.bulk_update(
                    products_to_update,
                    fields=[
                        "name", "year", "universe", "category", "sub_category",
                        "brand_lab", "lab_distributor", "range_name", "specificity",
                        "family", "sub_family", "tva_percentage", "free_access"
                    ]
                )

        # 4.4.3. Journalisation du batch traité
        # ---------------------------------------------------------------------
        print(f"Batch {start // batch_size + 1} processed: {end - start} records.")


# 5. Point d'entrée du script
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    main()
