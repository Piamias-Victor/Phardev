import json
import os
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv

load_dotenv()

# Configuration pour nouvelle API WinPharma
API_URL = os.environ.get('API_URL')  # ex: YXBvdGhpY2Fs
API_PASSWORD = os.environ.get('API_PASSWORD')  # ex: cGFzczE  
PHARMACY_ID = os.environ.get('PHARMACY_ID')  # ex: 062044623
SERVER_URL = os.environ.get('SERVER_URL')
BASE_URL = "https://grpstat.winpharma.com/ApiWp"

def handler(event, context, full_dump=False):  # Forcer False par défaut
    """
    Handler principal pour la nouvelle API WinPharma
    
    Args:
        event: Event Lambda
        context: Context Lambda  
        full_dump: Si True, récupère toutes les données. Si False, période récente.
    """
    print(f"🚀 Starting WinPharma NEW API handler")
    print(f"📋 Config: API_URL={API_URL}, PHARMACY_ID={PHARMACY_ID}")
    print(f"🔄 Full dump: {full_dump}")
    
    # Mapping des endpoints : API -> Django
    endpoints = {
        'produits': 'products',
        'achats': 'orders', 
        'ventes': 'sales'
    }
    
    results = {}
    
    for api_endpoint, django_endpoint in endpoints.items():
        print(f"\n📡 Processing {api_endpoint} -> {django_endpoint}")
        
        try:
            # Construire l'URL et les paramètres
            url = f"{BASE_URL}/{API_URL}/{api_endpoint}"
            params = {
                'password': API_PASSWORD,
                'Idnats': PHARMACY_ID
            }
            
            # Ajouter des paramètres temporels pour achats et ventes
            if not full_dump and api_endpoint in ['achats', 'ventes']:
                # MÊME PÉRIODE pour achats ET ventes : J-2 -> J-1 (2 jours)
                dt2 = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                dt1 = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
                
                params.update({'dt1': dt1, 'dt2': dt2})
                print(f"📅 Period: {dt1} -> {dt2}")
            
            # Appel à la nouvelle API
            print(f"🔍 Calling: {url}")
            print(f"📋 Params: {params}")
            
            response = requests.get(url, params=params, timeout=60)
            print(f"📊 Status: {response.status_code}")
            
            if response.status_code == 200:
                api_data = response.json()
                data_count = 0
                
                # Compter les données reçues
                if isinstance(api_data, list) and len(api_data) > 0:
                    pharmacy_data = api_data[0]
                    if api_endpoint in pharmacy_data:
                        data_count = len(pharmacy_data[api_endpoint])
                
                print(f"✅ Success: {data_count} records")
                
                # Envoyer à Django
                django_url = f"{SERVER_URL}/winpharma_new_api/create/{django_endpoint}"
                headers = {'Pharmacy-id': PHARMACY_ID}
                
                print(f"📤 Sending to: {django_url}")
                django_response = requests.post(
                    django_url, 
                    json=api_data,
                    headers=headers,
                    timeout=120
                )
                
                print(f"📥 Django response: {django_response.status_code}")
                
                if django_response.status_code == 200:
                    print(f"✅ {api_endpoint} processed successfully")
                    results[api_endpoint] = {
                        "status": "success",
                        "records": data_count,
                        "django_status": django_response.status_code
                    }
                else:
                    print(f"❌ Django error: {django_response.text}")
                    results[api_endpoint] = {
                        "status": "django_error",
                        "records": data_count,
                        "django_status": django_response.status_code,
                        "error": django_response.text
                    }
                    
            elif response.status_code == 400:
                error_msg = response.text
                print(f"⚠️ API Error 400: {error_msg}")
                results[api_endpoint] = {
                    "status": "api_error_400",
                    "error": error_msg
                }
                
            elif response.status_code == 204:
                print(f"ℹ️ No data available for {api_endpoint}")
                results[api_endpoint] = {
                    "status": "no_data",
                    "records": 0
                }
                
            else:
                print(f"❌ API Error {response.status_code}: {response.text}")
                results[api_endpoint] = {
                    "status": f"api_error_{response.status_code}",
                    "error": response.text
                }
                
        except requests.exceptions.Timeout:
            print(f"⏱️ Timeout for {api_endpoint}")
            results[api_endpoint] = {
                "status": "timeout",
                "error": "Request timeout"
            }
            
        except requests.exceptions.RequestException as e:
            print(f"🔌 Connection error for {api_endpoint}: {e}")
            results[api_endpoint] = {
                "status": "connection_error", 
                "error": str(e)
            }
            
        except Exception as e:
            print(f"💥 Unexpected error for {api_endpoint}: {e}")
            results[api_endpoint] = {
                "status": "unexpected_error",
                "error": str(e)
            }
    
    # Résumé final
    print(f"\n🏁 FINAL RESULTS:")
    success_count = sum(1 for r in results.values() if r.get("status") == "success")
    total_count = len(results)
    
    print(f"✅ Success: {success_count}/{total_count}")
    for endpoint, result in results.items():
        status = result.get("status", "unknown")
        records = result.get("records", 0)
        print(f"   {endpoint}: {status} ({records} records)")
    
    return {
        'statusCode': 200 if success_count > 0 else 500,
        'body': json.dumps({
            'message': f'Processed {success_count}/{total_count} endpoints successfully',
            'results': results,
            'pharmacy_id': PHARMACY_ID,
            'timestamp': datetime.now().isoformat()
        })
    }

def handler_full_dump(event, context):
    """Handler pour dump complet (tous les produits, période étendue pour ventes/achats)"""
    return handler(event, context, full_dump=True)

def handler_incremental(event, context):
    """Handler pour mise à jour incrémentale (période récente)"""
    return handler(event, context, full_dump=False)

# Handler par défaut
if __name__ == "__main__":
    # Test en local
    print("🧪 Testing locally...")
    result = handler({}, {}, full_dump=False)
    print(f"Result: {json.dumps(result, indent=2)}")