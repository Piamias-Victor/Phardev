import os
import re
import json
import gzip
from io import BytesIO
from datetime import datetime
from collections import defaultdict

import boto3
from botocore.exceptions import ClientError
from tqdm import tqdm

# Configuration du bucket et du préfixe
BUCKET_NAME = 'phardev'
FOLDER_PREFIX = 'Dexter_history/'  # Dossier à lire dans S3


def extract_date_from_key(key):
    """
    Extrait une date au format YYYY-MM-DD depuis le nom d'un fichier.

    Pour les fichiers dont le nom commence par "Vente",
    on suppose que le nom suit le schéma :
    "Vente_<cip_code>_<gers_code>_<datetimeIgnored>_<date_debut>_<date_fin>.json.gz"
    et on retourne la date de début (l'élément à l'index 5).

    Exemple : "Vente_2001842_2001842_2024-12-20-05-58-13_2023-12-01_2024-01-31.json.gz"
    renvoie un datetime pour 2023-12-01.
    """
    parts = key.split('_')
    if len(parts) >= 6:
        date_str = parts[5]
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError as e:
            print(f"Erreur de conversion pour {key} : {e}")
            return None
    else:
        print(f"Format inattendu pour le fichier {key}")
        return None


def list_files_in_folder(bucket_name, prefix):
    """
    Liste tous les objets dans un bucket S3 sous un préfixe donné.
    """
    s3_client = boto3.client('s3')
    files = []
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if 'Contents' in page:
                files.extend(page['Contents'])
    except ClientError as error:
        print(f"Erreur lors du listing des objets: {error}")
    return files


def group_files_by_month(files):
    """
    Regroupe les fichiers par mois (format 'YYYY-MM')
    en ne prenant en compte que ceux dont le nom de l'objet commence par "Vente".
    Retourne un dictionnaire avec les clés au format 'YYYY-MM' et les valeurs,
    les listes d'objets correspondantes.
    """
    files_by_month = defaultdict(list)

    for obj in files:
        key = obj.get('Key')
        # Ignorer le dossier lui-même ou les objets de taille 0
        if key == FOLDER_PREFIX or obj.get('Size', 0) == 0:
            continue

        # Récupération du nom de fichier (après Dexter_history/)
        filename = key.split('Dexter_history/')[1]

        # Si le nom de fichier ne commence pas par "Vente", on ignore
        if not filename.startswith("Vente"):
            continue

        file_date = extract_date_from_key(key)
        if file_date:
            month_str = file_date.strftime('%Y-%m')
            files_by_month[month_str].append(obj)
        else:
            print(f"Pas de date trouvée pour le fichier : {key}")

    return files_by_month


def process_files_by_month(files_by_month):
    """
    Parcourt le dictionnaire groupé par mois, lit l'intérieur de chaque fichier (en le décompressant et en extrayant le JSON),
    et additionne la valeur de "ventes" pour chaque pharmacie dans chaque mois.

    Retourne un dictionnaire :
      {
        'YYYY-MM': {
            'NomPharmacie1': total_ventes,
            'NomPharmacie2': total_ventes,
            ...
        },
        ...
      }
    """
    s3_client = boto3.client('s3')

    # Structure pour stocker {mois: {nom_pharmacie: total_ventes}}
    sales_by_month = defaultdict(lambda: defaultdict(int))

    for month, objects in sorted(files_by_month.items()):
        for obj in objects:
            key = obj.get('Key')
            try:
                response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
                compressed_body = response['Body'].read()

                with gzip.GzipFile(fileobj=BytesIO(compressed_body)) as gz:
                    json_content = json.load(gz)

                # On vérifie qu'on est sur la pharmacie voulue
                # Récupération du nom de la pharmacie
                pharmacy_name = json_content['organization'].get('nom_pharmacie', 'Pharmacie inconnue')

                # Suppose que json_content['ventes'] est un champ list ou similaire
                ventes_data = json_content.get('ventes', [])
                # Si c'est une liste, on prend simplement len(...)
                if isinstance(ventes_data, list):
                    ventes_count = len(ventes_data)
                else:
                    # Si jamais c'est autre chose, on tente de le convertir
                    ventes_count = 0

                # On incrémente le total pour (mois, nom_pharmacie)
                sales_by_month[month][pharmacy_name] += ventes_count

            except Exception as e:
                print(f"Erreur lors du traitement du fichier {key} : {e}")

    return sales_by_month


def main():
    print("Liste des fichiers dans le dossier Dexter_history :")
    files = list_files_in_folder(BUCKET_NAME, FOLDER_PREFIX)
    print(f"Total des fichiers trouvés : {len(files)}")

    files_by_month = group_files_by_month(files)
    monthly_sales = process_files_by_month(files_by_month)

    # Affichage final
    print("\n=== Récapitulatif des ventes par mois et par pharmacie ===")
    for month in sorted(monthly_sales.keys()):
        print(f"\nMois : {month}")
        for pharmacy_name, total_ventes in monthly_sales[month].items():
            print(f"   {pharmacy_name} : {total_ventes} ventes")


if __name__ == "__main__":
    main()
