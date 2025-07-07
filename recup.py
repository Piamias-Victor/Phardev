#!/usr/bin/env python3
"""
Script de récupération historique des données Winpharma 2
Période : Janvier 2024 -> Mai 2025
Pharmacie : 
"""

import requests
import json
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
from typing import List, Tuple
import logging
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('winpharma_historical.log'),
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

def fetch_winpharma_data(endpoint: str, dt1: str, dt2: str) -> dict:
    """
    Récupère les données depuis l'API Winpharma pour une période donnée
    
    Args:
        endpoint: 'produits', 'ventes', ou 'achats' 
        dt1: Date de début (YYYY-MM-DD)
        dt2: Date de fin (YYYY-MM-DD)
    
    Returns:
        dict: Données JSON de l'API
    """
    # Construction de l'URL selon la doc
    if endpoint == 'produits':
        # Pour produits, pas de dates requises selon la doc
        url = f"https://grpstat.winpharma.com/ApiWp/{API_URL}/{endpoint}?password={API_PASSWORD}&Idnats={PHARMACY_ID}"
    else:
        url = f"https://grpstat.winpharma.com/ApiWp/{API_URL}/{endpoint}?password={API_PASSWORD}&Idnats={PHARMACY_ID}&dt1={dt1}&dt2={dt2}"
    
    logger.info(f"Requête: {endpoint} du {dt1} au {dt2}")
    logger.debug(f"URL: {url}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"✅ {endpoint} {dt1}-{dt2}: {len(data)} enregistrements")
        return data
        
    except requests.exceptions.HTTPError as e:
        logger.error(f"❌ Erreur HTTP {response.status_code} pour {endpoint} {dt1}-{dt2}: {e}")
        logger.error(f"Réponse: {response.text}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Erreur de requête pour {endpoint} {dt1}-{dt2}: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"❌ Erreur de parsing JSON pour {endpoint} {dt1}-{dt2}: {e}")
        logger.error(f"Réponse brute: {response.text}")
        raise

def send_to_server(endpoint: str, data: dict) -> bool:
    """
    Envoie les données au serveur Django via les endpoints historiques
    
    Args:
        endpoint: 'products', 'orders', ou 'sales'
        data: Données à envoyer
    
    Returns:
        bool: True si succès, False sinon
    """
    url = f"{SERVER_URL}/winpharma_historical/create/{endpoint}"
    headers = {'Pharmacy-id': PHARMACY_ID, 'Content-Type': 'application/json'}
    
    try:
        response = requests.post(url, json=data, headers=headers, timeout=60)
        response.raise_for_status()
        
        logger.info(f"✅ Données {endpoint} envoyées avec succès")
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Erreur envoi {endpoint}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Réponse serveur: {e.response.text}")
        return False

def main():
    """Fonction principale du script"""
    
    logger.info("🚀 Début de la récupération historique Winpharma")
    logger.info(f"Pharmacie: {PHARMACY_ID}")
    logger.info(f"Période: {START_DATE.strftime('%Y-%m-%d')} -> {END_DATE.strftime('%Y-%m-%d')}")
    
    # Génération des périodes mensuelles
    periods = generate_monthly_periods(START_DATE, END_DATE)
    logger.info(f"📅 {len(periods)} périodes à traiter")
    
    # Compteurs pour le suivi
    total_requests = len(periods) * 2  # ventes + achats (on fait produits une seule fois)
    current_request = 0
    
    # 1. Récupération des produits (une seule fois)
    logger.info("\n" + "="*50)
    logger.info("📦 RÉCUPÉRATION DES PRODUITS")
    logger.info("="*50)
    
    try:
        products_data = fetch_winpharma_data('produits', '', '')
        success = send_to_server('products', products_data)
        if not success:
            logger.error("❌ Échec envoi produits - Arrêt du script")
            return
    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération des produits: {e}")
        return
    
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
            success = send_to_server('sales', ventes_data)
            if not success:
                logger.error(f"❌ Échec envoi ventes {dt1}-{dt2} - Arrêt du script")
                return
                
            logger.info(f"Progress: {current_request}/{total_requests} requêtes")
                
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération des ventes {dt1}-{dt2}: {e}")
            return
        
        # Récupération des achats  
        logger.info("🛒 Récupération des achats...")
        try:
            current_request += 1
            achats_data = fetch_winpharma_data('achats', dt1, dt2)
            success = send_to_server('orders', achats_data)
            if not success:
                logger.error(f"❌ Échec envoi achats {dt1}-{dt2} - Arrêt du script")
                return
                
            logger.info(f"Progress: {current_request}/{total_requests} requêtes")
                
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération des achats {dt1}-{dt2}: {e}")
            return
        
        # Petite pause entre les périodes pour éviter de surcharger l'API
        if i < len(periods):  # Pas de pause après la dernière période
            logger.info("⏸️  Pause de 2 secondes...")
            time.sleep(2)
    
    logger.info("\n" + "="*50)
    logger.info("🎉 RÉCUPÉRATION HISTORIQUE TERMINÉE AVEC SUCCÈS!")
    logger.info("="*50)
    logger.info(f"✅ {len(periods)} périodes traitées")
    logger.info(f"✅ {current_request} requêtes effectuées")

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
        raise