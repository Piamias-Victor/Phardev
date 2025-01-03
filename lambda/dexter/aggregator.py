import gzip
import json
import os
import traceback
from io import BytesIO

import boto3
import botocore
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

bucket_name = 'phardev'
subfolder_prefix = 'Dexter_history/'


def handler(event, context):
    s3_client = boto3.client('s3')
    aggregated_data = {}

    try:
        paginator = s3_client.get_paginator('list_objects_v2')

        for page in paginator.paginate(Bucket=bucket_name, Prefix=subfolder_prefix):
            if 'Contents' in page:
                for obj in tqdm(page['Contents']):
                    if obj['Key'] == subfolder_prefix or obj.get('Size', 0) == 0:
                        continue

                    try:
                        key = obj['Key']
                        filename = key.split('Dexter_history/')[1]

                        if not filename.startswith("Stock"):
                            continue

                        # Fetch and decompress file
                        response = s3_client.get_object(Bucket=bucket_name, Key=key)
                        compressed_body = response['Body'].read()

                        with gzip.GzipFile(fileobj=BytesIO(compressed_body)) as gz:
                            json_content = json.load(gz)

                        pharmacy_id = json_content['organization']['id_national']
                        if pharmacy_id not in aggregated_data:
                            aggregated_data[pharmacy_id] = []

                        for product in json_content['produits']:
                            if isinstance(product['code_produit'], list):
                                for code in product['code_produit']:
                                    if code.get('referent'):
                                        aggregated_data[pharmacy_id].append((product['produit_id'], code.get('code')))
                                        break

                    except Exception as e:
                        print(f"Failed to process file {obj['Key']}: {traceback.format_exc()}")
        for pharmacy_id, values in aggregated_data.items():
            with open(f'{pharmacy_id}_aggregated_data.json', 'w', encoding='utf-8') as f:
                json.dump(values, f, ensure_ascii=False, indent=4)

    except botocore.exceptions.ClientError as error:
        print(f"Error listing objects: {error}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error listing objects')
        }

handler(1, 1)
