import json
import os
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

load_dotenv()

api_key = "wservice"
api_password = os.environ.get('PASSWORD')
id_nat = os.environ.get('IDNAT')
SERVER_URL = os.environ.get('SERVER_URL')


def handler(event, context, full_dump=True):
    endpoints = {
        'produits': 'products',
        'commandes': 'orders',
        'ventes': 'sales'
    }

    for in_endpoint, out_endpoint in endpoints.items():
        url = f"https://apiwp.winpharma.com/query/{id_nat}/api/v1/{in_endpoint}"
        if not full_dump:
            last_week_datetime = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d%H%M')
            url = f"{url}?after={last_week_datetime}"

        try:
            print(url)
            response = requests.get(url, auth=HTTPBasicAuth(api_key, api_password))
            print(len(response.json()))
            x = requests.post(f"{SERVER_URL}/winpharma/create/{out_endpoint}", json=response.json(),
                              headers={'Pharmacy-id': id_nat})

            if x.status_code != 200:
                print(f"Error: {x.status_code}: {x.text}")

        except requests.exceptions.RequestException as e:
            print(f"Connexion Error: {e}")

handler(1,1)