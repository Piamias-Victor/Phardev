#!/usr/bin/env python3
"""
Test local de la Lambda nouvelle API
"""

import os
import sys
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Ajouter le répertoire courant au path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importer le handler
from app import handler

def test_local():
    """Test local de la Lambda"""
    print("🧪 TESTING LAMBDA LOCALLY")
    print("="*50)
    
    # Variables d'environnement pour test
    os.environ['API_URL'] = 'YXBvdGhpY2Fs'
    os.environ['API_PASSWORD'] = 'cGFzczE'
    os.environ['PHARMACY_ID'] = '832002810'
    os.environ['SERVER_URL'] = 'http://localhost:8000'  # Serveur local
    
    print(f"🔧 Config:")
    print(f"   API_URL: {os.environ.get('API_URL')}")
    print(f"   PHARMACY_ID: {os.environ.get('PHARMACY_ID')}")
    print(f"   SERVER_URL: {os.environ.get('SERVER_URL')}")
    
    try:
        # Test avec full_dump=False (plus rapide)
        print(f"\n🚀 Running handler (incremental)...")
        result = handler({}, {}, full_dump=False)
        
        print(f"\n📊 RESULTS:")
        print(f"   Status Code: {result['statusCode']}")
        
        import json
        body = json.loads(result['body'])
        print(f"   Message: {body['message']}")
        print(f"   Results:")
        
        for endpoint, details in body['results'].items():
            status = details.get('status', 'unknown')
            records = details.get('records', 0)
            print(f"      {endpoint}: {status} ({records} records)")
        
        return result
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    result = test_local()
    
    if result and result['statusCode'] == 200:
        print(f"\n✅ SUCCESS - Lambda test completed!")
    else:
        print(f"\n❌ FAILED - Check errors above")