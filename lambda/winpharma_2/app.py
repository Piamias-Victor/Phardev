import json
import os
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

load_dotenv()

SERVER_URL = os.environ.get('SERVER_URL')
PHARMACY_ID = os.environ.get('PHARMACY_ID')
PASSWORD = os.environ.get('PASSWORD')


def handler(event, context, full_dump=True):
    endpoints = {
        'produits': 'products',
        'commandes': 'orders',
        'ventes': 'sales'
    }

    for in_endpoint, out_endpoint in endpoints.items():
        url = f"https://grpstat.winpharma.com/ApiWp/{PHARMACY_ID}/{in_endpoint}?password={PASSWORD}&Idnats=832011373"
        if not full_dump:
            dt1 = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            dt2 = datetime.now().strftime('%Y-%m-%d')
            url += f"&dt1={dt1}&dt2={dt2}"

        try:
            print(url)
            response = requests.get(url)
            print(len(response.json()))
            x = requests.post(f"{SERVER_URL}/winpharma_2/create/{out_endpoint}", json=response.json(),
                              headers={'Pharmacy-id': PHARMACY_ID})

            if x.status_code != 200:
                print(f"Error: {x.status_code}: {x.text}")

        except requests.exceptions.RequestException as e:
            print(f"Connexion Error: {e}")

handler(1,1)