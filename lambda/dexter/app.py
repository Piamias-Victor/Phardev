import json
import botocore
import boto3
from dotenv import load_dotenv
import requests
import os
from io import BytesIO
import gzip
from datetime import datetime

load_dotenv()

SERVER_URL = os.environ.get('SERVER_URL')


def handler(event, context, full_dump=False):
    # Initialisez le client S3 avec Botocore
    s3_client = boto3.client('s3')

    # Nom du bucket et préfixe du sous-dossier
    bucket_name = 'phardev'
    subfolder_prefix = 'Dexter/'

    try:
        # Liste des objets avec le préfixe du sous-dossier
        paginator = s3_client.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': bucket_name, 'Prefix': subfolder_prefix}

        all_files = []

        for page in paginator.paginate(**operation_parameters):
            if 'Contents' in page:
                for obj in page['Contents']:
                    # Skip les "objets vides" correspondant au nom du répertoire
                    if obj['Key'] == subfolder_prefix or obj.get('Size', 0) == 0:
                        continue

                    # Extraire la date du fichier depuis son nom (format: Stock_<id>_<id>_<start_date>_<end_date>.json.gz)
                    try:
                        key = obj['Key']
                        filename = key.split('Dexter/')[1].split('.json.gz')[0]
                        parts = filename.split('_')

                        if len(parts) < 5:
                            continue

                        start_date_str = parts[4]  # Date au format "YYYY-MM-DD"
                        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                        all_files.append((start_date, obj))  # Ajoutez à la liste avec la date de début
                    except Exception as e:
                        print(f"Failed to parse file date for {obj['Key']}: {e}")
                        continue

        # Trier les fichiers par date extraite
        all_files.sort(key=lambda x: x[0])

        # Traiter les fichiers triés
        for file_date, obj in all_files:
            try:
                # Télécharger le fichier depuis S3
                response = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
                compressed_body = response['Body'].read()

                # Décompresser le fichier gzip
                with gzip.GzipFile(fileobj=BytesIO(compressed_body)) as gz:
                    json_content = json.load(gz)

                # Extraire l'endpoint à partir du nom du fichier
                key = obj['Key'].split('Dexter/')[1].split('.json.gz')[0]
                endpoint = key.split('_')[0].lower()

                if endpoint != 'achat':
                    continue

                print(f"Processing {obj['Key']}")

                # Envoyer la requête POST
                response = requests.post(f"{SERVER_URL}/dexter/create/{endpoint}", json=json_content)
                print(f"Response status for {obj['Key']}: {response.status_code}")
                   # Si la requête POST est un succès, déplacer le fichier vers Dexter_history/
                   #  if response.status_code == 200:
                   #      new_key = obj['Key'].replace(subfolder_prefix, 'Dexter_history/')
                   #      s3_client.copy_object(
                   #          Bucket=bucket_name,
                   #          CopySource={'Bucket': bucket_name, 'Key': obj['Key']},
                   #          Key=new_key
                   #      )
                   #      s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                   #      print(f"File {obj['Key']} successfully processed and moved to {new_key}.")
                   #  else:
                   #      print(f"POST request failed for {obj['Key']} with status {response.status_code}: {response.text}")
            except Exception as e:
                print(f"Failed to process file {obj['Key']}: {e}")
    except botocore.exceptions.ClientError as error:
        print(f"Error listing objects: {error}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error listing objects')
        }


handler(1, 1, True)
