#!/usr/bin/env python3
"""
Script de test pour valider la nouvelle API WinPharma
Teste les 3 endpoints : produits, achats (commandes), ventes
"""

import requests
import json
import logging
from datetime import datetime, timedelta

# Configuration des logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Credentials de test fournis par Everys
API_URL = "YXBvdGhpY2Fs"
API_PASSWORD = "cGFzczE"
IDNAT = "832011373"
BASE_URL = "https://grpstat.winpharma.com/ApiWp"

def test_api_endpoint(endpoint, params=None):
    """
    Teste un endpoint de la nouvelle API
    
    Args:
        endpoint: nom de l'endpoint (produits, achats, ventes)
        params: paramètres supplémentaires
    
    Returns:
        dict: réponse de l'API ou None si erreur
    """
    url = f"{BASE_URL}/{API_URL}/{endpoint}"
    
    # Paramètres de base
    base_params = {
        'password': API_PASSWORD,
        'Idnats': IDNAT
    }
    
    # Ajouter les paramètres supplémentaires
    if params:
        base_params.update(params)
    
    logger.info(f"🔍 Test endpoint: {endpoint}")
    logger.info(f"📡 URL: {url}")
    logger.info(f"📋 Params: {base_params}")
    
    try:
        response = requests.get(url, params=base_params, timeout=30)
        logger.info(f"📊 Status Code: {response.status_code}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                logger.info(f"✅ Success! Data type: {type(data)}")
                
                if isinstance(data, list) and len(data) > 0:
                    logger.info(f"📦 Response contains {len(data)} pharmacy record(s)")
                    
                    # Analyser la structure
                    first_record = data[0]
                    if isinstance(first_record, dict):
                        logger.info(f"🏥 Pharmacy ID: {first_record.get('cip_pharma', 'Unknown')}")
                        
                        # Compter les données par type
                        for key in ['produits', 'achats', 'ventes']:
                            if key in first_record:
                                count = len(first_record[key]) if isinstance(first_record[key], list) else 0
                                logger.info(f"📊 {key}: {count} records")
                                
                                # Afficher un échantillon des premières données
                                if count > 0:
                                    sample = first_record[key][0]
                                    logger.info(f"📋 Sample {key[:-1]}: {json.dumps(sample, indent=2)[:200]}...")
                
                return data
                
            except json.JSONDecodeError as e:
                logger.error(f"❌ JSON decode error: {e}")
                logger.error(f"📄 Raw response: {response.text[:500]}...")
                return None
                
        else:
            logger.error(f"❌ HTTP Error {response.status_code}")
            logger.error(f"📄 Response: {response.text}")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Request failed: {e}")
        return None

def test_produits():
    """Test l'endpoint produits (pas de paramètres temporels nécessaires)"""
    logger.info("\n" + "="*50)
    logger.info("🧪 TESTING PRODUITS ENDPOINT")
    logger.info("="*50)
    
    return test_api_endpoint("produits")

def test_achats():
    """Test l'endpoint achats avec période"""
    logger.info("\n" + "="*50)
    logger.info("🧪 TESTING ACHATS ENDPOINT")
    logger.info("="*50)
    
    # Utiliser une période récente
    dt2 = datetime.now().strftime('%Y-%m-%d')
    dt1 = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    params = {
        'dt1': dt1,
        'dt2': dt2
    }
    
    return test_api_endpoint("achats", params)

def test_ventes():
    """Test l'endpoint ventes avec période (utilise l'exemple fourni)"""
    logger.info("\n" + "="*50)
    logger.info("🧪 TESTING VENTES ENDPOINT")
    logger.info("="*50)
    
    # Utiliser la période de l'exemple fourni
    params = {
        'dt1': '2025-05-20',
        'dt2': '2025-05-24'
    }
    
    return test_api_endpoint("ventes", params)

def analyze_data_structure(data, endpoint_name):
    """
    Analyse la structure des données retournées pour valider notre mapping
    """
    if not data or not isinstance(data, list) or len(data) == 0:
        logger.warning(f"⚠️ No data to analyze for {endpoint_name}")
        return
    
    logger.info(f"\n📊 ANALYZING {endpoint_name.upper()} DATA STRUCTURE")
    logger.info("-" * 40)
    
    pharmacy_data = data[0]
    data_key = {
        'produits': 'produits',
        'achats': 'achats', 
        'ventes': 'ventes'
    }.get(endpoint_name, endpoint_name)
    
    if data_key not in pharmacy_data:
        logger.warning(f"⚠️ Key '{data_key}' not found in response")
        return
    
    items = pharmacy_data[data_key]
    if not items:
        logger.info(f"ℹ️ No {data_key} data available")
        return
    
    # Analyser le premier élément
    first_item = items[0]
    logger.info(f"🔍 First {data_key[:-1]} fields:")
    for key, value in first_item.items():
        value_type = type(value).__name__
        value_preview = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
        logger.info(f"   {key}: {value_type} = {value_preview}")
    
    # Pour les commandes, analyser aussi les lignes
    if endpoint_name == 'achats' and 'lignes' in first_item:
        lignes = first_item['lignes']
        if lignes:
            logger.info(f"🔍 First ligne fields:")
            for key, value in lignes[0].items():
                value_type = type(value).__name__
                value_preview = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                logger.info(f"   {key}: {value_type} = {value_preview}")

def main():
    """Fonction principale de test"""
    logger.info("🚀 DÉBUT DES TESTS DE LA NOUVELLE API WINPHARMA")
    logger.info("="*60)
    
    # Tests des 3 endpoints
    results = {}
    
    # Test produits
    results['produits'] = test_produits()
    if results['produits']:
        analyze_data_structure(results['produits'], 'produits')
    
    # Test achats
    results['achats'] = test_achats()
    if results['achats']:
        analyze_data_structure(results['achats'], 'achats')
    
    # Test ventes 
    results['ventes'] = test_ventes()
    if results['ventes']:
        analyze_data_structure(results['ventes'], 'ventes')
    
    # Résumé final
    logger.info("\n" + "="*60)
    logger.info("📋 RÉSUMÉ DES TESTS")
    logger.info("="*60)
    
    for endpoint, data in results.items():
        status = "✅ SUCCESS" if data else "❌ FAILED"
        logger.info(f"{endpoint}: {status}")
    
    logger.info("\n🏁 TESTS TERMINÉS")
    
    return results

if __name__ == "__main__":
    results = main()
    
    # Sauvegarder les résultats pour analyse
    with open('api_test_results.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print("\n💾 Résultats sauvegardés dans 'api_test_results.json'")