#!/usr/bin/env python3
"""
Test spécifique pour les achats avec gestion correcte des dates
"""

import requests
import json
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_URL = "YXBvdGhpY2Fs"
API_PASSWORD = "cGFzczE"
IDNAT = "832011373"
BASE_URL = "https://grpstat.winpharma.com/ApiWp"

def test_achats_with_correct_dates():
    """
    Test achats avec différentes stratégies de dates
    Gestion du délai de 24h et logique de période
    """
    logger.info("🧪 TESTING ACHATS avec dates ajustées")
    
    # Stratégie 1: Période récente mais avec marge de sécurité
    # Si on est le 23, dernière date dispo = 22, donc on teste 21-22
    available_date = datetime.now() - timedelta(days=1)  # 22 juin
    start_date = available_date - timedelta(days=1)      # 21 juin (début)
    
    dt1 = start_date.strftime('%Y-%m-%d')
    dt2 = available_date.strftime('%Y-%m-%d')
    
    logger.info(f"📅 Stratégie 1: {dt1} -> {dt2}")
    result1 = test_achats_period(dt1, dt2, "Période récente (J-2 -> J-1)")
    
    if not result1:
        # Stratégie 2: Période plus ancienne (1 semaine)
        dt2 = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')  # 21 juin
        dt1 = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')  # 15 juin
        
        logger.info(f"📅 Stratégie 2: {dt1} -> {dt2}")
        result2 = test_achats_period(dt1, dt2, "Période ancienne (J-8 -> J-2)")
        
        if not result2:
            # Stratégie 3: Période connue qui marche (exemple ventes)
            dt1 = "2025-05-20"
            dt2 = "2025-05-24"
            
            logger.info(f"📅 Stratégie 3: {dt1} -> {dt2}")
            result3 = test_achats_period(dt1, dt2, "Période connue (mai 2025)")
            return result3
        return result2
    return result1

def test_achats_period(dt1, dt2, description):
    """Test achats pour une période donnée"""
    url = f"{BASE_URL}/{API_URL}/achats"
    params = {
        'password': API_PASSWORD,
        'Idnats': IDNAT,
        'dt1': dt1,
        'dt2': dt2
    }
    
    logger.info(f"🔍 Test: {description}")
    logger.info(f"📡 URL: {url}")
    logger.info(f"📋 Params: {params}")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        logger.info(f"📊 Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"✅ Success! Data type: {type(data)}")
            
            if isinstance(data, list) and len(data) > 0:
                pharmacy_data = data[0]
                achats = pharmacy_data.get('achats', [])
                logger.info(f"📦 Found {len(achats)} achats")
                
                if achats:
                    # Analyser la structure du premier achat
                    first_achat = achats[0]
                    logger.info(f"🔍 First achat structure:")
                    for key, value in first_achat.items():
                        value_type = type(value).__name__
                        if key == 'lignes' and isinstance(value, list) and value:
                            logger.info(f"   {key}: {value_type} (length: {len(value)})")
                            # Analyser la première ligne
                            first_ligne = value[0]
                            logger.info(f"      First ligne: {json.dumps(first_ligne, indent=8)}")
                        else:
                            value_preview = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                            logger.info(f"   {key}: {value_type} = {value_preview}")
                
                return data
            else:
                logger.info(f"ℹ️ No achats data in response")
                return data
                
        elif response.status_code == 400:
            logger.error(f"❌ HTTP Error 400: {response.text}")
            return None
        elif response.status_code == 204:
            logger.info(f"ℹ️ No data available for period {dt1} -> {dt2}")
            return []
        else:
            logger.error(f"❌ HTTP Error {response.status_code}: {response.text}")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Request failed: {e}")
        return None

def main():
    logger.info("🚀 TEST ACHATS avec gestion correcte des dates")
    logger.info("="*60)
    
    result = test_achats_with_correct_dates()
    
    if result:
        logger.info("✅ ACHATS test réussi !")
        
        # Sauvegarder le résultat
        with open('achats_test_result.json', 'w') as f:
            json.dump(result, f, indent=2, default=str)
        logger.info("💾 Résultat sauvegardé dans 'achats_test_result.json'")
        
    else:
        logger.error("❌ Impossible de récupérer les données achats")
    
    return result

if __name__ == "__main__":
    result = main()