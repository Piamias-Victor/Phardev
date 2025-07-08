#!/usr/bin/env python3
"""
Script de récupération historique MULTI-PHARMACIES avec TVA - VERSION CORRIGÉE
Chaque pharmacie utilise ses propres credentials
Période : Janvier 2024 -> Juin 2025
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
        logging.FileHandler('winpharma_multi_pharmacies_correct.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration de base depuis le .env
SERVER_URL = os.getenv('SERVER_URL')

# Dates de début et fin
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 6, 30)

# 🆕 CONFIGURATION PHARMACIES AVEC LEURS CREDENTIALS
# Structure: {PHARMACY_ID: {"name": "...", "api_url": "...", "api_password": "..."}}

# Credentials communs (à tester)
DEFAULT_API_URL = "YXBvdGhpY2Fs"
DEFAULT_API_PASSWORD = "cGFzczE"

# Liste complète des pharmacies
ALL_PHARMACIES = {
    "o62044623": "GIE Grande Pharmacie Cap 3000",
    "062044623": "Grande Pharmacie de La Part DIEU", 
    "372006049": "Pharmacie Sirvin",
    "342030285": "Pharmacie du Centre",
    "132069444": "Pharmacie de la Montagnette",
    "132040585": "Pharmacie du Cours Mirabeau",
    "642013593": "Pharmacie BAB2",
    "062044623": "Pharmacie Becker Monteux",
    "062044623": "Pharmacie du 8 mai 1945",
    "342026218": "Pharmacie Espace Bocaud",
    "772012522": "Pharmacie Val D'Europe",
    "422027524": "Pharmacie de Monthieu",
    "682020763": "Pharmacie de la Croisière",
    "262071004": "Pharmacie Valence 2",
    "132046616": "Pharmacie Martinet",
    "062044623": "Phamracie Mouysset",
    "332018811": "Pharmacie du Chemin Long",
    "332022755": "Pharmacie de l'Etoile",
    "752040428": "Pharmacie de la Place de la République",
    "132066978": "Pharmacie Centrale",
    "832011373": "Pharmacie Varoise",
    "302003330": "Pharmacie de Castanet",
    "342026655": "SELARL Pharmacie des Arceaux",
    "672033586": "Pharmacie du Printemps",
    "202041711": "Pharmacie Taddei Medori",
    "o62037049": "Pharmacie Lingostière",
    "132028473": "Pharmacie Saint Jean",
    "302006531": "Pharmacie des Portes d'Uzès",
    "912015492": "Pharmacie Centrale Evry 2",
    "192005940": "Pharmacie Egletons",
    "302006192": "Pharmacie des Salicornes",
    "952700268": "Pharmacie Coté Seine",
    "202021481": "Pharmacie du Valinco",
    "772011623": "Pharmacie du Centre Dammarie Les Lys",
    "332022219": "Pharmacie de l'Alliance",
    "852007137": "Pharmacie Ylium",
    "422023671": "Pharmacie du Forez",
    "832011498": "Grande pharmacie hyéroise / Pharmacie Massillon",
    "732002811": "Pharmacie du Pradian",
    "422026542": "Pharmacie de l'Europe",
    "922020771": "Grande Pharmacie de la Station",
    "742005481": "Pharmacie du Leman",
    "o52702370": "Pharmacie de Tokoro",
    "922021373": "Pharmacie des Quatres Chemins",
    "952701043": "Pharmacie de la Muette",
    "752043471": "Pharmacie Faubourg Bastille",
    "912015369": "Grande Pharmacie de Fleury",
    "442002119": "Pharmacie de Beaulieu",
    "792020646": "Pharmacie du Bocage",
    "202040697": "Pharmacie de la Rocade",
    "312008915": "Grande Pharmacie des Arcades",
    "692013469": "Pharmacie Jalles",
    "342027588": "Pharmacie de Capestang",
    "842005456": "Pharmacie de la Sorgue",
    "842002008": "Pharmacie Becker Carpentras",
    "842003121": "Pharmacie de l'Ecluse",
    "202021648": "Pharmacie de Sarrola",
    "132081613": "Pharmacie des Ateliers",
    "842006348": "Pharmacie de Caumont",
    "132048687": "Pharmacie du Centre (Ventabren)",
    "732003132": "Pharmacie des Cascades",
    "662004100": "Pharmacie du Wahoo",
    "662004522": "Pharmacie du Château",
    "782712756": "Pharmacie de la Gare",
    "842006462": "Grande Pharmacie des Ocres",
    "342030137": "Pharmacie Montarnaud",
    "280003641": "Pharmacie du Géant Luce",
    "802006031": "Pharmacie Paque",
    "842005472": "Pharmacie Cap sud"
}

# Générer la configuration automatiquement avec les mêmes credentials
PHARMACIES_CONFIG = {
    pharmacy_id: {
        "name": pharmacy_name,
        "api_url": DEFAULT_API_URL,
        "api_password": DEFAULT_API_PASSWORD
    }
    for pharmacy_id, pharmacy_name in ALL_PHARMACIES.items()
}

def generate_monthly_periods(start_date: datetime, end_date: datetime) -> List[Tuple[str, str]]:
    """Génère les périodes mensuelles"""
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
        if month_start > end_date:
            break
    
    return periods

def test_pharmacy_credentials(pharmacy_id: str, api_url: str, api_password: str) -> bool:
    """Test si les credentials d'une pharmacie fonctionnent"""
    url = f"https://grpstat.winpharma.com/ApiWp/{api_url}/produits"
    params = {
        'password': api_password,
        'Idnats': pharmacy_id
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            logger.info(f"✅ Credentials OK pour {pharmacy_id}")
            return True
        elif response.status_code == 401:
            logger.error(f"❌ Credentials invalides pour {pharmacy_id}")
            return False
        else:
            logger.warning(f"⚠️ Réponse inattendue {response.status_code} pour {pharmacy_id}")
            return False
    except Exception as e:
        logger.error(f"❌ Erreur de test pour {pharmacy_id}: {e}")
        return False

def fetch_winpharma_data(endpoint: str, pharmacy_id: str, api_url: str, api_password: str, dt1: str = "", dt2: str = "") -> Optional[dict]:
    """Récupère les données depuis l'API Winpharma pour une pharmacie"""
    if endpoint == 'produits':
        url = f"https://grpstat.winpharma.com/ApiWp/{api_url}/{endpoint}"
        params = {'password': api_password, 'Idnats': pharmacy_id}
    else:
        url = f"https://grpstat.winpharma.com/ApiWp/{api_url}/{endpoint}"
        params = {'password': api_password, 'Idnats': pharmacy_id, 'dt1': dt1, 'dt2': dt2}
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"✅ {endpoint} {pharmacy_id} {dt1}-{dt2}: {len(json.dumps(data))} caractères")
                return data
                
            elif response.status_code == 204:
                logger.info(f"ℹ️ Pas de données pour {endpoint} {pharmacy_id} {dt1}-{dt2}")
                return []
                
            elif response.status_code == 400:
                error_text = response.text
                logger.warning(f"⚠️ Erreur 400 pour {endpoint} {pharmacy_id}: {error_text}")
                
                # Gestion des erreurs de date
                if "dt2 est postérieur" in error_text:
                    import re
                    match = re.search(r"dernière date disponible = (\d{4}-\d{2}-\d{2})", error_text)
                    if match:
                        max_date = match.group(1)
                        logger.info(f"🔄 Retry avec date max: {max_date}")
                        
                        params['dt2'] = max_date
                        response = requests.get(url, params=params, timeout=30)
                        if response.status_code == 200:
                            return response.json()
                
                return None
                
            elif response.status_code == 401:
                logger.error(f"❌ Accès refusé (401) pour {pharmacy_id} - Credentials invalides")
                return None
                
            else:
                logger.error(f"❌ Erreur HTTP {response.status_code} pour {pharmacy_id}: {response.text}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.info(f"⏸️ Retry dans {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erreur de requête pour {pharmacy_id} (tentative {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                time.sleep(wait_time)
                continue
            return None
    
    return None

def analyze_tva_data(data: dict) -> dict:
    """Analyse les données de ventes pour extraire les informations TVA"""
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

def send_to_server(endpoint: str, pharmacy_id: str, data: dict) -> bool:
    """Envoie les données au serveur Django"""
    url = f"{SERVER_URL}/winpharma_historical/create/{endpoint}"
    headers = {'Pharmacy-id': pharmacy_id, 'Content-Type': 'application/json'}
    
    try:
        response = requests.post(url, json=data, headers=headers, timeout=60)
        
        if response.status_code == 200:
            return True
        else:
            logger.error(f"❌ Erreur serveur {response.status_code} pour {pharmacy_id}: {response.text}")
            return False
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Erreur envoi {endpoint} pour {pharmacy_id}: {e}")
        return False

def process_single_pharmacy(pharmacy_id: str, config: dict, periods: List[Tuple[str, str]]) -> Dict:
    """Traite une seule pharmacie avec ses credentials spécifiques"""
    pharmacy_name = config['name']
    api_url = config['api_url']
    api_password = config['api_password']
    
    logger.info(f"\n🏥 TRAITEMENT: {pharmacy_name} ({pharmacy_id})")
    logger.info("="*80)
    
    # 1. Test des credentials
    if not test_pharmacy_credentials(pharmacy_id, api_url, api_password):
        logger.error(f"❌ Credentials invalides pour {pharmacy_name} - SKIP")
        return {
            'pharmacy_id': pharmacy_id,
            'pharmacy_name': pharmacy_name,
            'status': 'credentials_error',
            'products': {'success': 0, 'fail': 1},
            'sales': {'success': 0, 'fail': 0},
            'orders': {'success': 0, 'fail': 0}
        }
    
    pharmacy_stats = {
        'pharmacy_id': pharmacy_id,
        'pharmacy_name': pharmacy_name,
        'status': 'processing',
        'products': {'success': 0, 'fail': 0},
        'sales': {'success': 0, 'fail': 0},
        'orders': {'success': 0, 'fail': 0},
        'tva_stats': {
            'total_periods_with_tva': 0,
            'total_products_with_tva': set(),
            'all_tva_values': {}
        }
    }
    
    # 2. Récupération des produits
    logger.info("📦 Récupération des produits...")
    try:
        products_data = fetch_winpharma_data('produits', pharmacy_id, api_url, api_password)
        if products_data is not None:
            success = send_to_server('products', pharmacy_id, products_data)
            if success:
                pharmacy_stats['products']['success'] = 1
                logger.info("✅ Produits traités avec succès")
            else:
                pharmacy_stats['products']['fail'] = 1
                logger.error("❌ Échec envoi produits")
        else:
            pharmacy_stats['products']['fail'] = 1
            logger.error("❌ Échec récupération produits")
    except Exception as e:
        logger.error(f"❌ Exception produits: {e}")
        pharmacy_stats['products']['fail'] = 1
    
    # 3. Récupération des ventes et achats par période
    for i, (dt1, dt2) in enumerate(periods, 1):
        logger.info(f"📊 Période {i}/{len(periods)}: {dt1} -> {dt2}")
        
        # Ventes
        try:
            ventes_data = fetch_winpharma_data('ventes', pharmacy_id, api_url, api_password, dt1, dt2)
            
            if ventes_data is not None:
                # Analyser la TVA
                tva_stats = analyze_tva_data(ventes_data)
                if tva_stats['lignes_avec_tva'] > 0:
                    pharmacy_stats['tva_stats']['total_periods_with_tva'] += 1
                    pharmacy_stats['tva_stats']['total_products_with_tva'].update(tva_stats['produits_avec_tva'])
                    
                    for tva_val, count in tva_stats['tva_values'].items():
                        pharmacy_stats['tva_stats']['all_tva_values'][tva_val] = pharmacy_stats['tva_stats']['all_tva_values'].get(tva_val, 0) + count
                
                success = send_to_server('sales', pharmacy_id, ventes_data)
                if success:
                    pharmacy_stats['sales']['success'] += 1
                else:
                    pharmacy_stats['sales']['fail'] += 1
            else:
                pharmacy_stats['sales']['fail'] += 1
                
        except Exception as e:
            logger.error(f"❌ Exception ventes {dt1}-{dt2}: {e}")
            pharmacy_stats['sales']['fail'] += 1
        
        # Achats
        try:
            achats_data = fetch_winpharma_data('achats', pharmacy_id, api_url, api_password, dt1, dt2)
            
            if achats_data is not None:
                success = send_to_server('orders', pharmacy_id, achats_data)
                if success:
                    pharmacy_stats['orders']['success'] += 1
                else:
                    pharmacy_stats['orders']['fail'] += 1
            else:
                pharmacy_stats['orders']['fail'] += 1
                
        except Exception as e:
            logger.error(f"❌ Exception achats {dt1}-{dt2}: {e}")
            pharmacy_stats['orders']['fail'] += 1
        
        # Pause entre périodes
        if i < len(periods):
            time.sleep(1)
    
    # Statistiques finales de la pharmacie
    pharmacy_stats['status'] = 'completed'
    
    logger.info(f"\n📊 STATISTIQUES {pharmacy_name}:")
    for endpoint, stat in pharmacy_stats.items():
        if endpoint not in ['pharmacy_id', 'pharmacy_name', 'status', 'tva_stats']:
            total = stat['success'] + stat['fail']
            if total > 0:
                success_rate = (stat['success'] / total) * 100
                logger.info(f"   {endpoint.upper()}: {stat['success']}/{total} succès ({success_rate:.1f}%)")
    
    # Statistiques TVA
    tva_stats = pharmacy_stats['tva_stats']
    if tva_stats['total_periods_with_tva'] > 0:
        logger.info(f"   TVA: {len(tva_stats['total_products_with_tva'])} produits uniques avec TVA")
    
    return pharmacy_stats

def main():
    """Fonction principale du script multi-pharmacies"""
    
    logger.info("🚀 RÉCUPÉRATION HISTORIQUE MULTI-PHARMACIES AVEC CREDENTIALS INDIVIDUELS")
    logger.info(f"📅 Période: {START_DATE.strftime('%Y-%m-%d')} -> {END_DATE.strftime('%Y-%m-%d')}")
    logger.info(f"🏥 Nombre de pharmacies configurées: {len(PHARMACIES_CONFIG)}")
    
    # Génération des périodes
    periods = generate_monthly_periods(START_DATE, END_DATE)
    logger.info(f"📅 {len(periods)} périodes à traiter par pharmacie")
    
    # Afficher aperçu des pharmacies configurées
    logger.info(f"🔍 Pharmacies configurées:")
    for pharmacy_id, config in PHARMACIES_CONFIG.items():
        logger.info(f"   - {config['name']} ({pharmacy_id})")
    
    # ⚠️ AVERTISSEMENT IMPORTANT
    logger.warning("⚠️ IMPORTANT: Ce script ne traite que les pharmacies avec credentials configurés")
    logger.warning("⚠️ Pour ajouter d'autres pharmacies, vous devez obtenir leurs credentials API")
    
    confirm = input(f"\n❓ Traiter {len(PHARMACIES_CONFIG)} pharmacies sur {len(periods)} périodes ? (oui/non): ").lower().strip()
    
    if confirm not in ['oui', 'o', 'yes', 'y']:
        logger.info("⏹️ Traitement annulé")
        return
    
    # Statistiques globales
    global_stats = {
        'total_pharmacies': len(PHARMACIES_CONFIG),
        'pharmacies_processed': 0,
        'pharmacies_failed': 0,
        'pharmacies_credentials_error': 0,
        'total_tva_products': set(),
        'global_tva_values': {}
    }
    
    # Traitement de chaque pharmacie
    for pharmacy_id, config in PHARMACIES_CONFIG.items():
        try:
            pharmacy_stats = process_single_pharmacy(pharmacy_id, config, periods)
            
            if pharmacy_stats['status'] == 'credentials_error':
                global_stats['pharmacies_credentials_error'] += 1
            elif pharmacy_stats['status'] == 'completed':
                global_stats['pharmacies_processed'] += 1
                
                # Fusionner les stats TVA globales
                global_stats['total_tva_products'].update(pharmacy_stats['tva_stats']['total_products_with_tva'])
                for tva_val, count in pharmacy_stats['tva_stats']['all_tva_values'].items():
                    global_stats['global_tva_values'][tva_val] = global_stats['global_tva_values'].get(tva_val, 0) + count
            else:
                global_stats['pharmacies_failed'] += 1
            
            # Pause entre pharmacies
            logger.info("⏸️ Pause de 3 secondes avant la pharmacie suivante...")
            time.sleep(3)
            
        except Exception as e:
            logger.error(f"💥 Erreur fatale pour {config['name']}: {e}")
            global_stats['pharmacies_failed'] += 1
            continue
    
    # Statistiques finales
    logger.info("\n" + "="*80)
    logger.info("📊 STATISTIQUES FINALES GLOBALES")
    logger.info("="*80)
    
    logger.info(f"🏥 Pharmacies traitées: {global_stats['pharmacies_processed']}")
    logger.info(f"❌ Pharmacies échouées: {global_stats['pharmacies_failed']}")
    logger.info(f"🔑 Erreurs de credentials: {global_stats['pharmacies_credentials_error']}")
    
    if global_stats['total_tva_products']:
        logger.info(f"📊 TVA GLOBALE:")
        logger.info(f"   - Produits uniques avec TVA: {len(global_stats['total_tva_products'])}")
        logger.info(f"   - Valeurs TVA trouvées: {len(global_stats['global_tva_values'])}")
        
        # Top 10 des valeurs TVA
        sorted_tva = sorted(global_stats['global_tva_values'].items(), key=lambda x: x[1], reverse=True)[:10]
        logger.info(f"   📋 Top 10 des TVA les plus fréquentes:")
        for tva_val, count in sorted_tva:
            logger.info(f"     TVA {tva_val}%: {count} occurrences")
    
    # Recommandations finales
    logger.info(f"\n💡 RECOMMANDATIONS:")
    if global_stats['pharmacies_credentials_error'] > 0:
        logger.info(f"   - Contactez l'équipe WinPharma pour obtenir les credentials des autres pharmacies")
        logger.info(f"   - Ajoutez leurs credentials dans PHARMACIES_CONFIG")
    if global_stats['pharmacies_processed'] > 0:
        logger.info(f"   - {global_stats['pharmacies_processed']} pharmacies traitées avec succès ✅")

if __name__ == "__main__":
    # Vérification de la configuration
    if not SERVER_URL:
        logger.error("❌ Variable SERVER_URL manquante dans .env")
        exit(1)
    
    if not PHARMACIES_CONFIG:
        logger.error("❌ Aucune pharmacie configurée dans PHARMACIES_CONFIG")
        logger.error("💡 Ajoutez les credentials des pharmacies dans le script")
        exit(1)
    
    logger.info(f"🔧 Configuration chargée:")
    logger.info(f"   - SERVER_URL: {SERVER_URL}")
    logger.info(f"   - Pharmacies configurées: {len(PHARMACIES_CONFIG)}")
    
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n⚠️ Script interrompu par l'utilisateur")
    except Exception as e:
        logger.error(f"❌ Erreur fatale: {e}")
        import traceback
        traceback.print_exc()
        raise