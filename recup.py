#!/usr/bin/env python3
"""
Script de récupération historique des données Winpharma 2 - VERSION AVEC TVA
Période : Janvier 2024 -> Mai 2025
Pharmacie : Avec récupération de la TVA depuis les ventes
"""

import requests
import json
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
from typing import List, Tuple, Optional
import logging
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('winpharma_historical_with_tva.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration depuis les variables d'environnement
API_URL = os.getenv('API_URL')
API_PASSWORD = os.getenv('API_PASSWORD') 
PHARMACY_ID = os.getenv('PHARMACY_ID')
SERVER_URL = os.getenv('SERVER_URL')

# Dates de début et fin
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 5, 31)

def generate_monthly_periods(start_date: datetime, end_date: datetime) -> List[Tuple[str, str]]:
    """
    Génère les périodes mensuelles entre start_date et end_date
    Retourne une liste de tuples (dt1, dt2) au format YYYY-MM-DD
    """
    periods = []
    current_date = start_date.replace(day=1)  # Premier jour du mois
    
    while current_date <= end_date:
        # Premier jour du mois
        month_start = current_date
        # Dernier jour du mois
        month_end = (current_date + relativedelta(months=1)) - timedelta(days=1)
        
        # Si c'est le dernier mois, on s'arrête à end_date
        if month_end > end_date:
            month_end = end_date
            
        dt1 = month_start.strftime('%Y-%m-%d')
        dt2 = month_end.strftime('%Y-%m-%d')
        
        periods.append((dt1, dt2))
        logger.info(f"Période générée: {dt1} -> {dt2}")
        
        # Passer au mois suivant
        current_date = current_date + relativedelta(months=1)
        
        # Si on a atteint la fin, on s'arrête
        if month_start > end_date:
            break
    
    return periods

def test_single_request(endpoint: str, dt1: str = "", dt2: str = "") -> Optional[dict]:
    """
    Test une seule requête API pour diagnostic
    """
    if endpoint == 'produits':
        url = f"https://grpstat.winpharma.com/ApiWp/{API_URL}/{endpoint}?password={API_PASSWORD}&Idnats={PHARMACY_ID}"
    else:
        url = f"https://grpstat.winpharma.com/ApiWp/{API_URL}/{endpoint}?password={API_PASSWORD}&Idnats={PHARMACY_ID}&dt1={dt1}&dt2={dt2}"
    
    logger.info(f"🔍 TEST: {endpoint} - URL: {url}")
    
    try:
        response = requests.get(url, timeout=30)
        
        logger.info(f"📊 Status Code: {response.status_code}")
        logger.info(f"📋 Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                logger.info(f"✅ JSON parsé avec succès")
                logger.info(f"📈 Structure: {type(data)}")
                if isinstance(data, list) and len(data) > 0:
                    logger.info(f"📋 Premier élément: {list(data[0].keys()) if isinstance(data[0], dict) else type(data[0])}")
                    if endpoint in data[0]:
                        logger.info(f"📊 Nombre d'enregistrements: {len(data[0][endpoint])}")
                        
                        # 🆕 DIAGNOSTIC SPÉCIAL POUR LES VENTES (vérifier présence TVA)
                        if endpoint == 'ventes' and len(data[0][endpoint]) > 0:
                            vente_sample = data[0][endpoint][0]
                            if 'lignes' in vente_sample and len(vente_sample['lignes']) > 0:
                                ligne_sample = vente_sample['lignes'][0]
                                logger.info(f"🔍 Structure ligne de vente: {list(ligne_sample.keys())}")
                                if 'tva' in ligne_sample:
                                    logger.info(f"✅ TVA trouvée: {ligne_sample['tva']}")
                                else:
                                    logger.warning("⚠️ Pas de champ TVA dans les lignes de vente")
                        
                return data
            except json.JSONDecodeError as e:
                logger.error(f"❌ Erreur JSON: {e}")
                logger.error(f"📄 Réponse brute: {response.text[:500]}...")
                return None
        
        elif response.status_code == 204:
            logger.info(f"ℹ️ Pas de données (204)")
            return []
            
        elif response.status_code == 400:
            logger.error(f"❌ Erreur 400: {response.text}")
            return None
            
        else:
            logger.error(f"❌ Erreur {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"💥 Exception: {e}")
        return None

def fetch_winpharma_data(endpoint: str, dt1: str, dt2: str) -> Optional[dict]:
    """
    Récupère les données depuis l'API Winpharma pour une période donnée
    Version améliorée avec gestion d'erreurs et retry
    """
    if endpoint == 'produits':
        url = f"https://grpstat.winpharma.com/ApiWp/{API_URL}/{endpoint}?password={API_PASSWORD}&Idnats={PHARMACY_ID}"
    else:
        url = f"https://grpstat.winpharma.com/ApiWp/{API_URL}/{endpoint}?password={API_PASSWORD}&Idnats={PHARMACY_ID}&dt1={dt1}&dt2={dt2}"
    
    logger.info(f"Requête: {endpoint} du {dt1} au {dt2}")
    logger.debug(f"URL: {url}")
    
    # Retry avec backoff exponentiel
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                # 🆕 DIAGNOSTIC TVA POUR LES VENTES
                if endpoint == 'ventes' and isinstance(data, list) and len(data) > 0:
                    ventes = data[0].get('ventes', [])
                    tva_count = 0
                    total_lignes = 0
                    
                    for vente in ventes[:5]:  # Vérifier les 5 premières ventes
                        for ligne in vente.get('lignes', []):
                            total_lignes += 1
                            if 'tva' in ligne and ligne['tva'] is not None:
                                tva_count += 1
                    
                    if total_lignes > 0:
                        tva_ratio = (tva_count / total_lignes) * 100
                        logger.info(f"🔍 TVA présente dans {tva_count}/{total_lignes} lignes ({tva_ratio:.1f}%)")
                    
                logger.info(f"✅ {endpoint} {dt1}-{dt2}: {len(data)} enregistrements")
                return data
                
            elif response.status_code == 204:
                logger.info(f"ℹ️ Pas de données pour {endpoint} {dt1}-{dt2}")
                return []
                
            elif response.status_code == 400:
                error_text = response.text
                logger.warning(f"⚠️ Erreur 400 pour {endpoint}: {error_text}")
                
                # Gestion spéciale pour les erreurs de date
                if "dt2 est postérieur" in error_text:
                    import re
                    match = re.search(r"dernière date disponible = (\d{4}-\d{2}-\d{2})", error_text)
                    if match:
                        max_date = match.group(1)
                        logger.info(f"🔄 Retry avec date max: {max_date}")
                        
                        # Construire nouvelle URL avec date corrigée
                        if endpoint != 'produits':
                            url_corrected = f"https://grpstat.winpharma.com/ApiWp/{API_URL}/{endpoint}?password={API_PASSWORD}&Idnats={PHARMACY_ID}&dt1={dt1}&dt2={max_date}"
                            response = requests.get(url_corrected, timeout=30)
                            if response.status_code == 200:
                                data = response.json()
                                logger.info(f"✅ {endpoint} avec date corrigée: {len(data)} enregistrements")
                                return data
                
                return None
                
            else:
                logger.error(f"❌ Erreur HTTP {response.status_code} pour {endpoint}: {response.text}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.info(f"⏸️ Retry dans {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erreur de requête pour {endpoint} (tentative {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                logger.info(f"⏸️ Retry dans {wait_time}s...")
                time.sleep(wait_time)
                continue
            return None
        except json.JSONDecodeError as e:
            logger.error(f"❌ Erreur JSON pour {endpoint}: {e}")
            logger.error(f"📄 Réponse: {response.text[:500]}...")
            return None
    
    return None

def analyze_tva_data(data: dict) -> dict:
    """
    🆕 NOUVELLE FONCTION : Analyse les données de ventes pour extraire les informations TVA
    """
    tva_stats = {
        'total_ventes': 0,
        'total_lignes': 0,
        'lignes_avec_tva': 0,
        'tva_values': {},  # {tva_value: count}
        'produits_avec_tva': set()
    }
    
    if not isinstance(data, list) or len(data) == 0:
        return tva_stats
    
    ventes = data[0].get('ventes', [])
    tva_stats['total_ventes'] = len(ventes)
    
    for vente in ventes:
        for ligne in vente.get('lignes', []):
            tva_stats['total_lignes'] += 1
            
            # Vérifier présence TVA
            if 'tva' in ligne and ligne['tva'] is not None:
                tva_stats['lignes_avec_tva'] += 1
                
                # Compter les valeurs de TVA
                tva_value = ligne['tva']
                tva_stats['tva_values'][tva_value] = tva_stats['tva_values'].get(tva_value, 0) + 1
                
                # Ajouter le produit
                prod_id = ligne.get('prodId')
                if prod_id:
                    tva_stats['produits_avec_tva'].add(str(prod_id))
    
    return tva_stats

def send_to_server(endpoint: str, data: dict, diagnostic: bool = False) -> bool:
    """
    Envoie les données au serveur Django via les endpoints historiques
    Version améliorée avec diagnostic et analyse TVA
    """
    url = f"{SERVER_URL}/winpharma_historical/create/{endpoint}"
    headers = {'Pharmacy-id': PHARMACY_ID, 'Content-Type': 'application/json'}
    
    if diagnostic:
        logger.info(f"🔍 DIAGNOSTIC ENVOI:")
        logger.info(f"   URL: {url}")
        logger.info(f"   Headers: {headers}")
        logger.info(f"   Data type: {type(data)}")
        logger.info(f"   Data size: {len(json.dumps(data)) if data else 0} chars")
        
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
            logger.info(f"   Structure: {list(data[0].keys())}")
            
            # 🆕 ANALYSE SPÉCIALE POUR LES VENTES
            if endpoint == 'sales' and 'ventes' in data[0]:
                tva_stats = analyze_tva_data(data)
                logger.info(f"🔍 ANALYSE TVA:")
                logger.info(f"   - {tva_stats['total_ventes']} ventes")
                logger.info(f"   - {tva_stats['total_lignes']} lignes")
                logger.info(f"   - {tva_stats['lignes_avec_tva']} lignes avec TVA")
                logger.info(f"   - {len(tva_stats['produits_avec_tva'])} produits uniques avec TVA")
                
                if tva_stats['tva_values']:
                    logger.info(f"   - Valeurs TVA trouvées: {list(tva_stats['tva_values'].keys())}")
                    # Afficher les 5 valeurs TVA les plus fréquentes
                    sorted_tva = sorted(tva_stats['tva_values'].items(), key=lambda x: x[1], reverse=True)[:5]
                    for tva_val, count in sorted_tva:
                        logger.info(f"     TVA {tva_val}%: {count} occurrences")
    
    try:
        response = requests.post(url, json=data, headers=headers, timeout=60)
        
        logger.info(f"📊 Server response: {response.status_code}")
        
        if response.status_code == 200:
            logger.info(f"✅ Données {endpoint} envoyées avec succès")
            
            # 🆕 ANALYSER LA RÉPONSE DU SERVEUR POUR LES VENTES
            if endpoint == 'sales':
                try:
                    response_data = response.json()
                    logger.info(f"📋 Réponse serveur: {response_data.get('message', 'N/A')}")
                except:
                    pass
            
            return True
        else:
            logger.error(f"❌ Erreur serveur {response.status_code}: {response.text}")
            return False
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Erreur envoi {endpoint}: {e}")
        return False

def test_mode():
    """
    Mode test pour diagnostiquer les problèmes avec un seul endpoint
    🆕 AMÉLIORÉ avec analyse TVA
    """
    logger.info("🧪 MODE TEST - Diagnostic des endpoints avec analyse TVA")
    
    # Test avec une période récente
    dt1 = "2025-05-20"
    dt2 = "2025-05-24"
    
    for endpoint in ['produits', 'ventes', 'achats']:
        logger.info(f"\n{'='*50}")
        logger.info(f"🔍 TEST: {endpoint}")
        logger.info(f"{'='*50}")
        
        if endpoint == 'produits':
            data = test_single_request(endpoint)
        else:
            data = test_single_request(endpoint, dt1, dt2)
        
        if data is not None:
            # 🆕 ANALYSE TVA SPÉCIALE POUR LES VENTES
            if endpoint == 'ventes':
                logger.info("\n🔍 ANALYSE DÉTAILLÉE TVA:")
                tva_stats = analyze_tva_data(data)
                
                logger.info(f"📊 Statistiques TVA:")
                logger.info(f"   - Total ventes: {tva_stats['total_ventes']}")
                logger.info(f"   - Total lignes: {tva_stats['total_lignes']}")
                logger.info(f"   - Lignes avec TVA: {tva_stats['lignes_avec_tva']}")
                
                if tva_stats['total_lignes'] > 0:
                    tva_coverage = (tva_stats['lignes_avec_tva'] / tva_stats['total_lignes']) * 100
                    logger.info(f"   - Couverture TVA: {tva_coverage:.1f}%")
                
                logger.info(f"   - Produits uniques avec TVA: {len(tva_stats['produits_avec_tva'])}")
                
                if tva_stats['tva_values']:
                    logger.info(f"📋 Valeurs TVA détectées:")
                    for tva_val, count in sorted(tva_stats['tva_values'].items()):
                        logger.info(f"   - TVA {tva_val}%: {count} occurrences")
            
            # Mapper vers les endpoints Django
            django_endpoint = {
                'produits': 'products',
                'ventes': 'sales', 
                'achats': 'orders'
            }[endpoint]
            
            logger.info(f"🚀 Test envoi vers Django: {django_endpoint}")
            success = send_to_server(django_endpoint, data, diagnostic=True)
            
            if success:
                logger.info(f"✅ {endpoint} -> {django_endpoint} : OK")
            else:
                logger.error(f"❌ {endpoint} -> {django_endpoint} : ÉCHEC")
        else:
            logger.error(f"❌ Pas de données pour {endpoint}")

def main():
    """Fonction principale du script"""
    
    logger.info("🚀 Début de la récupération historique Winpharma AVEC TVA")
    logger.info(f"Pharmacie: {PHARMACY_ID}")
    logger.info(f"Période: {START_DATE.strftime('%Y-%m-%d')} -> {END_DATE.strftime('%Y-%m-%d')}")
    
    # Mode test pour diagnostiquer
    test_mode_enabled = input("🧪 Activer le mode test/diagnostic ? (o/n): ").lower() == 'o'
    
    if test_mode_enabled:
        test_mode()
        return
    
    # Génération des périodes mensuelles
    periods = generate_monthly_periods(START_DATE, END_DATE)
    logger.info(f"📅 {len(periods)} périodes à traiter")
    
    # Compteurs pour le suivi
    total_requests = len(periods) * 2  # ventes + achats (on fait produits une seule fois)
    current_request = 0
    
    # Stats de succès/échec
    stats = {
        'products': {'success': 0, 'fail': 0},
        'sales': {'success': 0, 'fail': 0},
        'orders': {'success': 0, 'fail': 0}
    }
    
    # 🆕 STATS TVA GLOBALES
    global_tva_stats = {
        'total_periods_with_tva': 0,
        'total_products_with_tva': set(),
        'all_tva_values': {}
    }
    
    # 1. Récupération des produits (une seule fois)
    logger.info("\n" + "="*50)
    logger.info("📦 RÉCUPÉRATION DES PRODUITS")
    logger.info("="*50)
    
    try:
        products_data = fetch_winpharma_data('produits', '', '')
        if products_data is not None:
            success = send_to_server('products', products_data)
            if success:
                stats['products']['success'] += 1
                logger.info("✅ Produits traités avec succès")
            else:
                stats['products']['fail'] += 1
                logger.error("❌ Échec envoi produits - Continuer quand même...")
        else:
            stats['products']['fail'] += 1
            logger.error("❌ Échec récupération produits")
    except Exception as e:
        logger.error(f"❌ Exception produits: {e}")
        stats['products']['fail'] += 1
    
    # 2. Récupération des ventes et achats par période
    for i, (dt1, dt2) in enumerate(periods, 1):
        logger.info(f"\n" + "="*50)
        logger.info(f"📊 PÉRIODE {i}/{len(periods)}: {dt1} -> {dt2}")
        logger.info("="*50)
        
        # Récupération des ventes
        logger.info("💰 Récupération des ventes...")
        try:
            current_request += 1
            ventes_data = fetch_winpharma_data('ventes', dt1, dt2)
            
            if ventes_data is not None:
                # 🆕 ANALYSER LA TVA AVANT ENVOI
                tva_stats = analyze_tva_data(ventes_data)
                if tva_stats['lignes_avec_tva'] > 0:
                    global_tva_stats['total_periods_with_tva'] += 1
                    global_tva_stats['total_products_with_tva'].update(tva_stats['produits_avec_tva'])
                    
                    # Fusionner les valeurs TVA
                    for tva_val, count in tva_stats['tva_values'].items():
                        global_tva_stats['all_tva_values'][tva_val] = global_tva_stats['all_tva_values'].get(tva_val, 0) + count
                    
                    logger.info(f"🔍 TVA cette période: {tva_stats['lignes_avec_tva']} lignes, {len(tva_stats['produits_avec_tva'])} produits")
                
                success = send_to_server('sales', ventes_data)
                if success:
                    stats['sales']['success'] += 1
                    logger.info(f"✅ Ventes {dt1}-{dt2} traitées")
                else:
                    stats['sales']['fail'] += 1
                    logger.error(f"❌ Échec envoi ventes {dt1}-{dt2}")
            else:
                stats['sales']['fail'] += 1
                logger.error(f"❌ Échec récupération ventes {dt1}-{dt2}")
                
            logger.info(f"Progress: {current_request}/{total_requests} requêtes")
                
        except Exception as e:
            logger.error(f"❌ Exception ventes {dt1}-{dt2}: {e}")
            stats['sales']['fail'] += 1
        
        # Récupération des achats  
        logger.info("🛒 Récupération des achats...")
        try:
            current_request += 1
            achats_data = fetch_winpharma_data('achats', dt1, dt2)
            
            if achats_data is not None:
                success = send_to_server('orders', achats_data)
                if success:
                    stats['orders']['success'] += 1
                    logger.info(f"✅ Achats {dt1}-{dt2} traités")
                else:
                    stats['orders']['fail'] += 1
                    logger.error(f"❌ Échec envoi achats {dt1}-{dt2}")
            else:
                stats['orders']['fail'] += 1
                logger.error(f"❌ Échec récupération achats {dt1}-{dt2}")
                
            logger.info(f"Progress: {current_request}/{total_requests} requêtes")
                
        except Exception as e:
            logger.error(f"❌ Exception achats {dt1}-{dt2}: {e}")
            stats['orders']['fail'] += 1
        
        # Petite pause entre les périodes pour éviter de surcharger l'API
        if i < len(periods):  # Pas de pause après la dernière période
            logger.info("⏸️  Pause de 2 secondes...")
            time.sleep(2)
    
    # 🆕 STATISTIQUES FINALES AVEC TVA
    logger.info("\n" + "="*50)
    logger.info("📊 STATISTIQUES FINALES")
    logger.info("="*50)
    
    for endpoint, stat in stats.items():
        total = stat['success'] + stat['fail']
        if total > 0:
            success_rate = (stat['success'] / total) * 100
            logger.info(f"{endpoint.upper()}: {stat['success']}/{total} succès ({success_rate:.1f}%)")
        else:
            logger.info(f"{endpoint.upper()}: Aucune donnée traitée")
    
    logger.info(f"✅ {current_request} requêtes effectuées")
    
    # 🆕 STATISTIQUES TVA GLOBALES
    logger.info(f"\n📊 STATISTIQUES TVA GLOBALES:")
    logger.info(f"   - Périodes avec TVA: {global_tva_stats['total_periods_with_tva']}/{len(periods)}")
    logger.info(f"   - Produits uniques avec TVA: {len(global_tva_stats['total_products_with_tva'])}")
    
    if global_tva_stats['all_tva_values']:
        logger.info(f"   - Valeurs TVA trouvées: {len(global_tva_stats['all_tva_values'])}")
        
        # Top 10 des valeurs TVA les plus fréquentes
        sorted_tva = sorted(global_tva_stats['all_tva_values'].items(), key=lambda x: x[1], reverse=True)[:10]
        logger.info(f"   📋 Top 10 des TVA les plus fréquentes:")
        for tva_val, count in sorted_tva:
            logger.info(f"     TVA {tva_val}%: {count} occurrences")

if __name__ == "__main__":
    # Vérification des variables d'environnement
    if not all([API_URL, API_PASSWORD, PHARMACY_ID, SERVER_URL]):
        logger.error("❌ Variables d'environnement manquantes:")
        if not API_URL:
            logger.error("   - API_URL")
        if not API_PASSWORD:
            logger.error("   - API_PASSWORD") 
        if not PHARMACY_ID:
            logger.error("   - PHARMACY_ID")
        if not SERVER_URL:
            logger.error("   - SERVER_URL")
        exit(1)
    
    logger.info(f"🔧 Configuration chargée:")
    logger.info(f"   - API_URL: {API_URL}")
    logger.info(f"   - PHARMACY_ID: {PHARMACY_ID}")
    logger.info(f"   - SERVER_URL: {SERVER_URL}")
    
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n⚠️  Script interrompu par l'utilisateur")
    except Exception as e:
        logger.error(f"❌ Erreur fatale: {e}")
        import traceback
        traceback.print_exc()
        raise