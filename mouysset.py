#!/usr/bin/env python3
"""
Script spécialisé pour Pharmacie Mouysset - Année 2025 uniquement
Récupération : Produits → Ventes → Achats (par mois)
Création automatique de "Pharmacie Mouysset V2"
"""

import requests
import json
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
from typing import List, Tuple, Optional, Dict
import logging
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mouysset_2025.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration de base
SERVER_URL = os.getenv('SERVER_URL')

# Configuration spécifique Mouysset
MOUYSSET_CONFIG = {
    "original_id": "832002810",
    "original_name": "Pharmacie Mouysset", 
    "new_name": "Pharmacie Mouysset V2",
    "api_url": "YXBvdGhpY2Fs",
    "api_password": "cGFzczE"
}

# Période 2025 uniquement
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2025, 12, 31)

def generate_monthly_periods(start_date: datetime, end_date: datetime) -> List[Tuple[str, str]]:
    """Génère les périodes mensuelles pour 2025"""
    periods = []
    current_date = start_date.replace(day=1)
    
    while current_date <= end_date:
        month_start = current_date
        month_end = (current_date + relativedelta(months=1)) - timedelta(days=1)
        
        if month_end > end_date:
            month_end = end_date
            
        dt1 = month_start.strftime('%Y-%m-%d')
        dt2 = month_end.strftime('%Y-%m-%d')
        periods.append((dt1, dt2))
        
        current_date = current_date + relativedelta(months=1)
    
    return periods

def test_pharmacy_credentials(pharmacy_id: str, api_url: str, api_password: str) -> bool:
    """Test si les credentials fonctionnent"""
    url = f"https://grpstat.winpharma.com/ApiWp/{api_url}/produits"
    params = {
        'password': api_password,
        'Idnats': pharmacy_id
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            logger.info(f"✅ Credentials OK pour {pharmacy_id}")
            return True
        elif response.status_code == 401:
            logger.error(f"❌ Credentials invalides pour {pharmacy_id}")
            return False
        else:
            logger.warning(f"⚠️ Réponse inattendue {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"❌ Erreur test credentials: {e}")
        return False

def fetch_winpharma_data(endpoint: str, pharmacy_id: str, api_url: str, api_password: str, dt1: str = "", dt2: str = "") -> Optional[dict]:
    """Récupère les données depuis l'API Winpharma avec retry"""
    if endpoint == 'produits':
        url = f"https://grpstat.winpharma.com/ApiWp/{api_url}/{endpoint}"
        params = {'password': api_password, 'Idnats': pharmacy_id}
        logger.info(f"📦 Récupération produits...")
    else:
        url = f"https://grpstat.winpharma.com/ApiWp/{api_url}/{endpoint}"
        params = {'password': api_password, 'Idnats': pharmacy_id, 'dt1': dt1, 'dt2': dt2}
        logger.info(f"📊 Récupération {endpoint} {dt1} → {dt2}")
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=60)
            
            if response.status_code == 200:
                data = response.json()
                size = len(json.dumps(data))
                logger.info(f"✅ {endpoint}: {size} caractères récupérés")
                return data
                
            elif response.status_code == 204:
                logger.info(f"ℹ️ Pas de données pour {endpoint} {dt1}-{dt2}")
                return []
                
            elif response.status_code == 400:
                error_text = response.text
                logger.warning(f"⚠️ Erreur 400: {error_text}")
                
                # Gestion erreur de date (ajuster dt2 si nécessaire)
                if "dt2 est postérieur" in error_text:
                    import re
                    match = re.search(r"dernière date disponible = (\d{4}-\d{2}-\d{2})", error_text)
                    if match:
                        max_date = match.group(1)
                        logger.info(f"🔄 Retry avec date max: {max_date}")
                        params['dt2'] = max_date
                        response = requests.get(url, params=params, timeout=60)
                        if response.status_code == 200:
                            return response.json()
                return None
                
            elif response.status_code == 401:
                logger.error(f"❌ Accès refusé - Credentials invalides")
                return None
                
            else:
                logger.error(f"❌ Erreur HTTP {response.status_code}: {response.text}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.info(f"⏸️ Retry dans {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erreur requête (tentative {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                time.sleep(wait_time)
                continue
            return None
    
    return None

def create_pharmacy_v2(new_name: str) -> bool:
    """Crée la nouvelle pharmacie avec le nom fourni"""
    url = f"{SERVER_URL}/api/pharmacy/create"
    payload = {
        "name": new_name
    }
    
    try:
        response = requests.post(url, json=payload, headers={'Content-Type': 'application/json'}, timeout=30)
        
        if response.status_code in [200, 201]:
            result = response.json()
            logger.info(f"✅ Pharmacie: {result.get('message', 'Créée')}")
            logger.info(f"   ID: {result.get('pharmacy_id', 'Non fourni')}")
            return True
        else:
            logger.error(f"❌ Erreur création pharmacie {response.status_code}: {response.text}")
            return False
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Erreur création pharmacie: {e}")
        return False

def send_to_server(endpoint: str, data: dict) -> bool:
    """Envoie les données au serveur Django avec header pharmacie V2"""
    url = f"{SERVER_URL}/winpharma_historical/create/{endpoint}"
    headers = {
        'Pharmacy-Name': MOUYSSET_CONFIG["new_name"],
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.post(url, json=data, headers=headers, timeout=120)
        
        if response.status_code == 200:
            logger.info(f"✅ {endpoint} envoyé avec succès")
            return True
        else:
            logger.error(f"❌ Erreur serveur {response.status_code}: {response.text}")
            return False
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Erreur envoi {endpoint}: {e}")
        return False

def analyze_tva_data(data: dict) -> dict:
    """Analyse les données TVA dans les ventes"""
    tva_stats = {
        'total_ventes': 0,
        'total_lignes': 0,
        'lignes_avec_tva': 0,
        'tva_values': {},
        'produits_avec_tva': set()
    }
    
    if not isinstance(data, list) or len(data) == 0:
        return tva_stats
    
    ventes = data[0].get('ventes', [])
    tva_stats['total_ventes'] = len(ventes)
    
    for vente in ventes:
        for ligne in vente.get('lignes', []):
            tva_stats['total_lignes'] += 1
            
            if 'tva' in ligne and ligne['tva'] is not None:
                tva_stats['lignes_avec_tva'] += 1
                tva_value = ligne['tva']
                tva_stats['tva_values'][tva_value] = tva_stats['tva_values'].get(tva_value, 0) + 1
                
                prod_id = ligne.get('prodId')
                if prod_id:
                    tva_stats['produits_avec_tva'].add(str(prod_id))
    
    return tva_stats

def main():
    """Fonction principale pour Pharmacie Mouysset 2025"""
    
    logger.info("=" * 80)
    logger.info("🏥 SCRIPT PHARMACIE MOUYSSET - ANNÉE 2025 UNIQUEMENT")
    logger.info("=" * 80)
    logger.info(f"📅 Période: {START_DATE.strftime('%Y-%m-%d')} → {END_DATE.strftime('%Y-%m-%d')}")
    logger.info(f"🏥 Pharmacie: {MOUYSSET_CONFIG['original_name']} → {MOUYSSET_CONFIG['new_name']}")
    
    # Génération des périodes mensuelles pour 2025
    periods = generate_monthly_periods(START_DATE, END_DATE)
    logger.info(f"📅 {len(periods)} mois à traiter: {[period[0][:7] for period in periods]}")
    
    # 1. TEST DES CREDENTIALS
    logger.info("\n🔑 ÉTAPE 1/5 - Test des credentials WinPharma")
    if not test_pharmacy_credentials(
        MOUYSSET_CONFIG["original_id"], 
        MOUYSSET_CONFIG["api_url"], 
        MOUYSSET_CONFIG["api_password"]
    ):
        logger.error("❌ Credentials invalides - ARRÊT du script")
        return
    
    # 2. CRÉATION PHARMACIE V2
    logger.info("\n🏥 ÉTAPE 2/5 - Création Pharmacie Mouysset V2")
    if not create_pharmacy_v2(MOUYSSET_CONFIG["new_name"]):
        logger.error("❌ Impossible de créer la pharmacie V2 - ARRÊT")
        return
    
    # 3. RÉCUPÉRATION PRODUITS
    logger.info("\n📦 ÉTAPE 3/5 - Récupération base produits")
    try:
        products_data = fetch_winpharma_data(
            'produits', 
            MOUYSSET_CONFIG["original_id"], 
            MOUYSSET_CONFIG["api_url"], 
            MOUYSSET_CONFIG["api_password"]
        )
        
        if products_data is not None:
            success = send_to_server('products', products_data)
            if not success:
                logger.error("❌ Échec envoi produits - CONTINUE malgré tout")
        else:
            logger.error("❌ Échec récupération produits")
    except Exception as e:
        logger.error(f"💥 Exception produits: {e}")
    
    # 4. RÉCUPÉRATION VENTES PAR MOIS
    logger.info("\n💰 ÉTAPE 4/5 - Récupération ventes 2025 (mois par mois)")
    
    stats_ventes = {
        'success': 0,
        'fail': 0,
        'total_tva_products': set(),
        'all_tva_values': {}
    }
    
    for i, (dt1, dt2) in enumerate(periods, 1):
        logger.info(f"📊 Ventes mois {i}/{len(periods)}: {dt1} → {dt2}")
        
        try:
            ventes_data = fetch_winpharma_data(
                'ventes', 
                MOUYSSET_CONFIG["original_id"], 
                MOUYSSET_CONFIG["api_url"], 
                MOUYSSET_CONFIG["api_password"], 
                dt1, dt2
            )
            
            if ventes_data is not None:
                # Analyse TVA
                tva_stats = analyze_tva_data(ventes_data)
                if tva_stats['lignes_avec_tva'] > 0:
                    stats_ventes['total_tva_products'].update(tva_stats['produits_avec_tva'])
                    for tva_val, count in tva_stats['tva_values'].items():
                        stats_ventes['all_tva_values'][tva_val] = stats_ventes['all_tva_values'].get(tva_val, 0) + count
                    logger.info(f"   📊 TVA: {tva_stats['lignes_avec_tva']} lignes avec TVA")
                
                success = send_to_server('sales', ventes_data)
                if success:
                    stats_ventes['success'] += 1
                else:
                    stats_ventes['fail'] += 1
            else:
                stats_ventes['fail'] += 1
                
        except Exception as e:
            logger.error(f"💥 Exception ventes {dt1}-{dt2}: {e}")
            stats_ventes['fail'] += 1
        
        # Pause entre mois
        time.sleep(2)
    
    # 5. RÉCUPÉRATION ACHATS PAR MOIS  
    logger.info("\n🛒 ÉTAPE 5/5 - Récupération achats 2025 (mois par mois)")
    
    stats_achats = {'success': 0, 'fail': 0}
    
    for i, (dt1, dt2) in enumerate(periods, 1):
        logger.info(f"📦 Achats mois {i}/{len(periods)}: {dt1} → {dt2}")
        
        try:
            achats_data = fetch_winpharma_data(
                'achats', 
                MOUYSSET_CONFIG["original_id"], 
                MOUYSSET_CONFIG["api_url"], 
                MOUYSSET_CONFIG["api_password"], 
                dt1, dt2
            )
            
            if achats_data is not None:
                success = send_to_server('orders', achats_data)
                if success:
                    stats_achats['success'] += 1
                else:
                    stats_achats['fail'] += 1
            else:
                stats_achats['fail'] += 1
                
        except Exception as e:
            logger.error(f"💥 Exception achats {dt1}-{dt2}: {e}")
            stats_achats['fail'] += 1
        
        # Pause entre mois
        time.sleep(2)
    
    # STATISTIQUES FINALES
    logger.info("\n" + "=" * 80)
    logger.info("📊 RÉSULTATS FINAUX - PHARMACIE MOUYSSET V2")
    logger.info("=" * 80)
    
    total_ventes = stats_ventes['success'] + stats_ventes['fail']
    total_achats = stats_achats['success'] + stats_achats['fail']
    
    logger.info(f"💰 VENTES: {stats_ventes['success']}/{total_ventes} mois traités avec succès")
    if total_ventes > 0:
        success_rate_ventes = (stats_ventes['success'] / total_ventes) * 100
        logger.info(f"   Taux de succès: {success_rate_ventes:.1f}%")
    
    logger.info(f"🛒 ACHATS: {stats_achats['success']}/{total_achats} mois traités avec succès")
    if total_achats > 0:
        success_rate_achats = (stats_achats['success'] / total_achats) * 100
        logger.info(f"   Taux de succès: {success_rate_achats:.1f}%")
    
    # Statistiques TVA
    if stats_ventes['total_tva_products']:
        logger.info(f"📊 TVA DÉTECTÉE:")
        logger.info(f"   Produits uniques avec TVA: {len(stats_ventes['total_tva_products'])}")
        
        # Top 5 des TVA les plus fréquentes
        sorted_tva = sorted(stats_ventes['all_tva_values'].items(), key=lambda x: x[1], reverse=True)[:5]
        logger.info(f"   Top 5 des TVA:")
        for tva_val, count in sorted_tva:
            logger.info(f"     TVA {tva_val}%: {count} occurrences")
    
    logger.info("\n✅ SCRIPT TERMINÉ - Pharmacie Mouysset V2 créée pour 2025")

if __name__ == "__main__":
    # Vérifications préliminaires
    if not SERVER_URL:
        logger.error("❌ Variable SERVER_URL manquante dans .env")
        exit(1)
    
    logger.info(f"🔧 Configuration:")
    logger.info(f"   SERVER_URL: {SERVER_URL}")
    logger.info(f"   Pharmacie source: {MOUYSSET_CONFIG['original_name']} ({MOUYSSET_CONFIG['original_id']})")
    logger.info(f"   Pharmacie destination: {MOUYSSET_CONFIG['new_name']}")
    
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n⚠️ Script interrompu par l'utilisateur")
    except Exception as e:
        logger.error(f"❌ Erreur fatale: {e}")
        import traceback
        traceback.print_exc()
        raise