import gzip
import json
import os
from datetime import datetime
from io import BytesIO

import boto3
import botocore
import requests
from dotenv import load_dotenv

load_dotenv()

SERVER_URL = os.environ.get('SERVER_URL')

endpoint_priority = {
    'stock': 0,
    'achat': 1,
    'vente': 2
}
bucket_name = 'phardev'
subfolder_prefix = 'Dexter/'


def handler(event, context):
    s3_client = boto3.client('s3')
    bucket_name = 'phardev'
    subfolder_prefix = 'Dexter/'

    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        all_files = []

        for page in paginator.paginate(Bucket=bucket_name, Prefix=subfolder_prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'] == subfolder_prefix or obj.get('Size', 0) == 0:
                        continue

                    try:
                        key = obj['Key']
                        filename = key.split('Dexter/')[1].split('.json.gz')[0]
                        parts = filename.split('_')

                        if len(parts) < 6:
                            continue

                        file_type = parts[0].lower()  # Stock, Achat, Vente
                        cip_code = parts[1]  # Code CIP Pharmagest
                        gers_code = parts[2]  # Code GERS
                        end_date_str = parts[5]  # Date de fin de la période

                        # Convertir les dates
                        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

                        all_files.append((cip_code, gers_code, end_date, file_type, obj))

                    except Exception as e:
                        print(f"Failed to parse file date for {obj['Key']}: {e}")

        # Trier les fichiers par pharmacie (CIP + GERS), puis par date d'extraction, puis par type (Stock, Achat, Vente)

        all_files.sort(key=lambda x: (x[0], x[1], x[2], endpoint_priority[x[3]]))

        # Traitement des fichiers triés
        for cip_code, gers_code, start_date, file_type, obj in all_files:
            try:
                response = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
                compressed_body = response['Body'].read()

                with gzip.GzipFile(fileobj=BytesIO(compressed_body)) as gz:
                    json_content = json.load(gz)

                print(f"Processing {obj['Key']}")

                response = requests.post(f"{SERVER_URL}/dexter/create/{file_type}", json=json_content)
                print(f"{response.status_code}")
                if response.status_code == 200:
                    new_key = obj['Key'].replace(subfolder_prefix, 'Dexter_history/')
                    s3_client.copy_object(
                        Bucket=bucket_name,
                        CopySource={'Bucket': bucket_name, 'Key': obj['Key']},
                        Key=new_key
                    )
                    s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                    print(f"File {obj['Key']} moved.")
                else:
                    print(f"POST failed for {obj['Key']}: {response.status_code}")

            except Exception as e:
                print(f"Failed to process file {obj['Key']}: {e}")

    except botocore.exceptions.ClientError as error:
        print(f"Error listing objects: {error}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error listing objects')
        }


handler(1, 1)
