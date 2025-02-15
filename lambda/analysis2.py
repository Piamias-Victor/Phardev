import os
import json
import gzip
from io import BytesIO

import boto3
from botocore.exceptions import ClientError
from tqdm import tqdm

BUCKET_NAME = 'phardev'
FOLDER_PREFIX = 'Dexter_history/'


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


def main():
    s3_client = boto3.client('s3')

    # Récupération de tous les fichiers dans le bucket/prefix
    all_files = list_files_in_folder(BUCKET_NAME, FOLDER_PREFIX)
    print(f"Total des fichiers trouvés : {len(all_files)}")

    for file_info in tqdm(all_files):
        key = file_info.get('Key', '')
        size = file_info.get('Size', 0)

        # Ignorer le "dossier" lui-même ou les objets vides
        if key == FOLDER_PREFIX or size == 0:
            continue

        # On ne traite que les fichiers commençant par "Stock"
        filename = key.split('Dexter_history/')[-1]

        # Lecture et décompression du fichier
        try:
            response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
            compressed_body = response['Body'].read()

            with gzip.GzipFile(fileobj=BytesIO(compressed_body)) as gz:
                json_content = json.load(gz)

            # Vérifier la pharmacie
            pharmacy_name = json_content['organization'].get('nom_pharmacie', 'Pharmacie inconnue')
            if pharmacy_name != 'PHARMACIE GRAND LITTORAL':
                continue

            # Conversion en texte et recherche de "3760196530022"
            if '3760196530022' in json.dumps(json_content):
                print(filename)

        except Exception as e:
            print(f"Erreur lors du traitement du fichier {key} : {e}")


if __name__ == "__main__":
    main()
