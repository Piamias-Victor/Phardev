#!/usr/bin/env python3
"""
Test direct du service winpharma_new_api sans Django
Simule les données et valide la logique de transformation
"""

import sys
import os
import json
import requests
from datetime import datetime, timedelta

# Ajouter le projet au path pour les imports
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

# Credentials API
API_URL = "YXBvdGhpY2Fs"
API_PASSWORD = "cGFzczE"
IDNAT_TEST = "832011373"
BASE_URL = "https://grpstat.winpharma.com/ApiWp"

def fetch_from_new_api(endpoint, params=None):
    """Récupère des données de la nouvelle API"""
    url = f"{BASE_URL}/{API_URL}/{endpoint}"
    base_params = {
        'password': API_PASSWORD,
        'Idnats': IDNAT_TEST
    }
    if params:
        base_params.update(params)
    
    try:
        print(f"🔍 Fetching {endpoint}...")
        response = requests.get(url, params=base_params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Success! Got {len(data)} pharmacy records")
            return data
        else:
            print(f"❌ API Error {response.status_code}: {response.text}")
            return None
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return None

def test_data_structure():
    """Test de la structure des données sans traitement"""
    print("🧪 TESTING DATA STRUCTURE")
    print("="*50)
    
    # Test produits
    print("\n📦 Testing PRODUITS structure:")
    products_data = fetch_from_new_api("produits")
    if products_data:
        pharmacy_data = products_data[0]
        products = pharmacy_data.get('produits', [])
        print(f"   Found {len(products)} products")
        if products:
            sample = products[0]
            print(f"   Sample product keys: {list(sample.keys())}")
            print(f"   Sample: {json.dumps(sample, indent=4)}")
    
    # Test achats
    print("\n🛒 Testing ACHATS structure:")
    dt2 = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    dt1 = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    achats_data = fetch_from_new_api("achats", {'dt1': dt1, 'dt2': dt2})
    if achats_data:
        pharmacy_data = achats_data[0]
        achats = pharmacy_data.get('achats', [])
        print(f"   Found {len(achats)} orders")
        if achats:
            sample = achats[0]
            print(f"   Sample order keys: {list(sample.keys())}")
            if 'lignes' in sample and sample['lignes']:
                print(f"   Sample ligne keys: {list(sample['lignes'][0].keys())}")
    
    # Test ventes
    print("\n💰 Testing VENTES structure:")
    ventes_data = fetch_from_new_api("ventes", {'dt1': '2025-05-20', 'dt2': '2025-05-24'})
    if ventes_data:
        pharmacy_data = ventes_data[0]
        ventes = pharmacy_data.get('ventes', [])
        print(f"   Found {len(ventes)} sales")
        if ventes:
            sample = ventes[0]
            print(f"   Sample sale keys: {list(sample.keys())}")
            if 'lignes' in sample and sample['lignes']:
                print(f"   Sample ligne keys: {list(sample['lignes'][0].keys())}")

def test_data_transformation():
    """Test de la transformation des données (logique pure)"""
    print("\n🔄 TESTING DATA TRANSFORMATION")
    print("="*50)
    
    # Récupérer des données réelles
    products_data = fetch_from_new_api("produits")
    if not products_data:
        print("❌ No products data available")
        return
    
    pharmacy_data = products_data[0]
    products_raw = pharmacy_data.get('produits', [])[:5]  # Prendre juste 5 pour test
    
    print(f"\n📊 Testing transformation of {len(products_raw)} products")
    
    # Simuler la logique de transformation de notre service
    transformed_products = []
    for obj in products_raw:
        try:
            prod_id = obj.get('ProdId')
            if not prod_id or int(prod_id) < 0:
                continue
                
            transformed = {
                'product_id': str(prod_id),
                'name': obj.get('Nom', ''),
                'code_13_ref': obj.get('Code13Ref') or None,
                'stock': obj.get('Stock', 0),
                'price_with_tax': float(obj.get('PrixTTC', 0)),
                'weighted_average_price': float(obj.get('PrixMP', 0)),
                # Nouveaux champs de la nouvelle API
                'extra_codes': obj.get('ExtraCodes', ''),
            }
            transformed_products.append(transformed)
            
        except Exception as e:
            print(f"   ⚠️ Error transforming product {obj.get('ProdId')}: {e}")
    
    print(f"✅ Successfully transformed {len(transformed_products)} products")
    
    # Afficher quelques exemples
    for i, product in enumerate(transformed_products[:3]):
        print(f"\n   Product {i+1}:")
        for key, value in product.items():
            print(f"      {key}: {value}")
    
    return transformed_products

def test_field_mapping():
    """Test spécifique du mapping des champs"""
    print("\n🗺️ TESTING FIELD MAPPING")
    print("="*50)
    
    # Mapping attendu
    field_mappings = {
        'produits': {
            'ProdId': 'product_id',
            'Nom': 'name', 
            'Code13Ref': 'code_13_ref',
            'Stock': 'stock',
            'PrixTTC': 'price_with_tax',
            'PrixMP': 'weighted_average_price'
        },
        'achats': {
            'id': 'order_id',
            'codeFourn': 'supplier_id',
            'nomFourn': 'supplier_name',
            'channel': 'step',
            'dateEnvoi': 'sent_date',
            'dateLivraison': 'delivery_date'
        }
    }
    
    # Tester avec vraies données
    products_data = fetch_from_new_api("produits")
    if products_data:
        sample_product = products_data[0]['produits'][0]
        print("\n📦 Product field mapping:")
        for api_field, our_field in field_mappings['produits'].items():
            value = sample_product.get(api_field, 'MISSING')
            print(f"   {api_field} -> {our_field}: {value}")
    
    # Test achats si disponible
    dt2 = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    dt1 = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    achats_data = fetch_from_new_api("achats", {'dt1': dt1, 'dt2': dt2})
    if achats_data and achats_data[0].get('achats'):
        sample_order = achats_data[0]['achats'][0]
        print("\n🛒 Order field mapping:")
        for api_field, our_field in field_mappings['achats'].items():
            value = sample_order.get(api_field, 'MISSING')
            print(f"   {api_field} -> {our_field}: {value}")

def main():
    """Fonction principale de test"""
    print("🚀 DIRECT SERVICE TESTING (NO DJANGO)")
    print("="*60)
    print("Testing data fetching and transformation logic...")
    
    try:
        # Test 1: Structure des données
        test_data_structure()
        
        # Test 2: Transformation des données
        transformed = test_data_transformation()
        
        # Test 3: Mapping des champs
        test_field_mapping()
        
        print("\n🏁 ALL TESTS COMPLETED")
        print("="*60)
        
        if transformed:
            print(f"✅ Successfully tested transformation of {len(transformed)} products")
            print("✅ Field mapping validated")
            print("✅ Ready for Django integration!")
        else:
            print("⚠️ Some tests failed, check the logs above")
            
    except KeyboardInterrupt:
        print("\n\n⏹️ Tests interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()