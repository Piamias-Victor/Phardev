#!/usr/bin/env python3
"""
Script simple pour créer uniquement la pharmacie Mouysset V2
"""

import requests
import os
import logging
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
SERVER_URL = os.getenv('SERVER_URL')
PHARMACY_NAME = "Pharmacie Mouysset V2"
PHARMACY_ID_NAT = "832002810"  # ← AJOUTER CETTE LIGNE

def create_pharmacy(pharmacy_name: str, id_nat: str) -> bool:  # ← Ajouter id_nat
    """Crée une nouvelle pharmacie"""
    url = f"{SERVER_URL}/api/pharmacy/create"
    payload = {"name": pharmacy_name, "id_nat": id_nat}  # ← Utiliser le paramètre
    
    logger.info(f"🏥 Création de la pharmacie: {pharmacy_name}")
    logger.info(f"🔢 ID National: {id_nat}")  # ← Ajouter ce log
    logger.info(f"🌐 URL: {url}")
    logger.info(f"📦 Payload: {payload}")
    
    try:
        response = requests.post(
            url, 
            json=payload, 
            headers={'Content-Type': 'application/json'}, 
            timeout=30
        )
        
        logger.info(f"📊 Status Code: {response.status_code}")
        logger.info(f"📄 Response Text: {response.text}")
        
        if response.status_code in [200, 201]:
            result = response.json()
            logger.info("✅ SUCCÈS!")
            logger.info(f"   Message: {result.get('message', 'N/A')}")
            logger.info(f"   ID: {result.get('pharmacy_id', 'N/A')}")
            logger.info(f"   Status: {result.get('status', 'N/A')}")
            return True
        else:
            logger.error(f"❌ ÉCHEC - Status: {response.status_code}")
            logger.error(f"   Response: {response.text}")
            return False
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ ERREUR DE REQUÊTE: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ ERREUR GÉNÉRALE: {e}")
        return False

def main():
    logger.info("=" * 60)
    logger.info("🏥 SCRIPT CRÉATION PHARMACIE SIMPLE")
    logger.info("=" * 60)
    
    if not SERVER_URL:
        logger.error("❌ Variable SERVER_URL manquante dans .env")
        return
    
    logger.info(f"🔧 Configuration:")
    logger.info(f"   SERVER_URL: {SERVER_URL}")
    logger.info(f"   Pharmacie à créer: {PHARMACY_NAME}")
    logger.info(f"   ID National: {PHARMACY_ID_NAT}")
    
    success = create_pharmacy(PHARMACY_NAME, PHARMACY_ID_NAT)
    
    if success:
        logger.info("🎉 SCRIPT TERMINÉ AVEC SUCCÈS")
    else:
        logger.error("💥 SCRIPT TERMINÉ AVEC ERREUR")

if __name__ == "__main__":
    main()