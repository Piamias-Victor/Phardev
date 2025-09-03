#!/usr/bin/env python3
"""
Script pour créer la pharmacie Mouysset V2, produits, ventes ET achats
Version avec option achats d'avril à août 2025
"""

import requests
import os
import logging
import json
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('pharmacie_v2_complete.log')
    ]
)
logger = logging.getLogger(__name__)

# Configuration
SERVER_URL = "http://127.0.0.1:8000"
PHARMACY_NAME = "Pharmacie Mouysset V2"
PHARMACY_ID_NAT = "832002810"

# Période ventes août 2025
AUGUST_SALES_PERIOD = {
    "dt1": "2025-08-01",
    "dt2": "2025-08-31"
}

# Période achats avril-août 2025
ORDERS_PERIOD = {
    "dt1": "2025-04-01",
    "dt2": "2025-08-31"
}

def create_pharmacy_only(pharmacy_name: str, id_nat: str) -> dict:
    """Crée uniquement la pharmacie"""
    url = f"{SERVER_URL}/api/v2/pharmacy/create"
    payload = {"name": pharmacy_name, "id_nat": id_nat}
    
    print(f"🏥 Création de la pharmacie: {pharmacy_name}")
    print(f"🔢 ID National: {id_nat}")
    print(f"🌐 URL: {url}")
    
    try:
        response = requests.post(url, json=payload, headers={'Content-Type': 'application/json'})
        print(f"📊 Status Code: {response.status_code}")
        
        if response.status_code in [200, 201]:
            result = response.json()
            print("✅ Pharmacie OK!")
            print(f"   Message: {result.get('message', 'N/A')}")
            print(f"   ID: {result.get('pharmacy_id', 'N/A')}")
            print(f"   Status: {result.get('status', 'N/A')}")
            return result
        else:
            logger.error(f"❌ Erreur pharmacie - Status: {response.status_code}")
            logger.error(f"   Response: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Erreur requête pharmacie: {e}")
        return None

def fetch_products(id_nat: str) -> dict:
    """Récupère les produits d'une pharmacie"""
    url = f"{SERVER_URL}/api/v2/pharmacy/products/fetch"
    payload = {"id_nat": id_nat}
    
    print(f"📦 Récupération des produits pour: {id_nat}")
    print(f"🌐 URL: {url}")
    
    try:
        response = requests.post(url, json=payload, headers={'Content-Type': 'application/json'})
        print(f"📊 Status Code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("✅ Produits OK!")
            print(f"   Message: {result.get('message', 'N/A')}")
            print(f"   Stats: {result.get('stats', {})}")
            return result
        else:
            logger.error(f"❌ Erreur produits - Status: {response.status_code}")
            logger.error(f"   Response: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Erreur requête produits: {e}")
        return None

def fetch_sales(id_nat: str, dt1: str, dt2: str) -> dict:
    """Récupère les ventes d'une pharmacie pour une période"""
    url = f"{SERVER_URL}/api/v2/pharmacy/sales/fetch"
    payload = {"id_nat": id_nat, "dt1": dt1, "dt2": dt2}
    
    print(f"💰 Récupération des ventes pour: {id_nat}")
    print(f"📅 Période: {dt1} -> {dt2}")
    print(f"🌐 URL: {url}")
    
    try:
        response = requests.post(url, json=payload, headers={'Content-Type': 'application/json'})
        print(f"📊 Status Code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("✅ Ventes OK!")
            print(f"   Message: {result.get('message', 'N/A')}")
            print(f"   Stats: {result.get('stats', {})}")
            return result
        else:
            logger.error(f"❌ Erreur ventes - Status: {response.status_code}")
            logger.error(f"   Response: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Erreur requête ventes: {e}")
        return None

def fetch_orders(id_nat: str, dt1: str, dt2: str) -> dict:
    """Récupère les achats d'une pharmacie pour une période"""
    url = f"{SERVER_URL}/api/v2/pharmacy/orders/fetch"
    payload = {"id_nat": id_nat, "dt1": dt1, "dt2": dt2}
    
    print(f"🛒 Récupération des achats pour: {id_nat}")
    print(f"📅 Période: {dt1} -> {dt2}")
    print(f"🌐 URL: {url}")
    
    try:
        response = requests.post(url, json=payload, headers={'Content-Type': 'application/json'})
        print(f"📊 Status Code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("✅ Achats OK!")
            print(f"   Message: {result.get('message', 'N/A')}")
            print(f"   Stats: {result.get('stats', {})}")
            return result
        else:
            logger.error(f"❌ Erreur achats - Status: {response.status_code}")
            logger.error(f"   Response: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Erreur requête achats: {e}")
        return None

def create_and_fetch_products(pharmacy_name: str, id_nat: str) -> dict:
    """Créer la pharmacie ET récupérer les produits"""
    url = f"{SERVER_URL}/api/v2/pharmacy/create-and-fetch"
    payload = {"name": pharmacy_name, "id_nat": id_nat}
    
    print(f"🚀 Traitement pharmacie + produits: {pharmacy_name}")
    print(f"🔢 ID National: {id_nat}")
    print(f"🌐 URL: {url}")
    
    try:
        response = requests.post(url, json=payload, headers={'Content-Type': 'application/json'})
        print(f"📊 Status Code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("✅ Traitement pharmacie + produits terminé!")
            
            pharmacy_info = result.get('pharmacy', {})
            print("🏥 Pharmacie:")
            print(f"   Message: {pharmacy_info.get('message', 'N/A')}")
            print(f"   Status: {pharmacy_info.get('status', 'N/A')}")
            
            products_info = result.get('products', {})
            print("📦 Produits:")
            print(f"   Message: {products_info.get('message', 'N/A')}")
            print(f"   Status: {products_info.get('status', 'N/A')}")
            print(f"   Stats: {products_info.get('stats', {})}")
            
            return result
        else:
            logger.error(f"❌ Erreur traitement - Status: {response.status_code}")
            try:
                error_data = response.json()
                logger.error(f"   Erreur JSON: {json.dumps(error_data, indent=2)}")
            except:
                logger.error(f"   Response text: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Erreur requête: {e}")
        return None

def create_products_sales_and_orders_complete(pharmacy_name: str, id_nat: str, sales_period: dict, orders_period: dict) -> dict:
    """Créer la pharmacie, récupérer les produits, ventes ET achats - ULTRA COMPLET"""
    url = f"{SERVER_URL}/api/v2/pharmacy/create-products-sales"
    payload = {
        "name": pharmacy_name, 
        "id_nat": id_nat,
        "sales_period": sales_period
    }
    
    print(f"🚀 Traitement ULTRA COMPLET: {pharmacy_name}")
    print(f"🔢 ID National: {id_nat}")
    print(f"📅 Ventes: {sales_period['dt1']} -> {sales_period['dt2']}")
    print(f"🛒 Achats: {orders_period['dt1']} -> {orders_period['dt2']}")
    print(f"🌐 URL: {url}")
    logger.warning("⏰ ATTENTION: Traitement très long (pharmacie + produits + ventes + achats)")
    
    try:
        # 1. Traitement pharmacie + produits + ventes
        response = requests.post(url, json=payload, headers={'Content-Type': 'application/json'})
        print(f"📊 Status Code pharmacie+produits+ventes: {response.status_code}")
        
        if response.status_code != 200:
            logger.error(f"❌ Erreur traitement initial: {response.status_code}")
            return None
            
        result = response.json()
        
        # Afficher les résultats intermédiaires
        pharmacy_info = result.get('pharmacy', {})
        products_info = result.get('products', {})
        sales_info = result.get('sales', {})
        
        print("✅ Traitement pharmacie + produits + ventes terminé!")
        print("🏥 Pharmacie:")
        print(f"   Status: {pharmacy_info.get('status', 'N/A')}")
        print("📦 Produits:")
        print(f"   Status: {products_info.get('status', 'N/A')}")
        print("💰 Ventes:")
        print(f"   Status: {sales_info.get('status', 'N/A')}")
        
        # 2. Récupération des achats
        print("🛒 Démarrage récupération des achats...")
        orders_result = fetch_orders(id_nat, orders_period['dt1'], orders_period['dt2'])
        
        if orders_result:
            orders_info = {
                'message': orders_result.get('message', 'Achats traités avec succès'),
                'status': 'success',
                'stats': orders_result.get('stats', {})
            }
            print("✅ Achats terminés avec succès!")
        else:
            orders_info = {
                'message': 'Erreur lors du traitement des achats',
                'status': 'error',
                'stats': {}
            }
            logger.error("❌ Échec traitement achats")
        
        # 3. Résultat final combiné
        final_result = {
            'pharmacy': pharmacy_info,
            'products': products_info,
            'sales': sales_info,
            'orders': orders_info,
            'sales_period': f"{sales_period['dt1']} -> {sales_period['dt2']}",
            'orders_period': f"{orders_period['dt1']} -> {orders_period['dt2']}",
            'overall_status': 'success' if all(
                info.get('status') == 'success' 
                for info in [products_info, sales_info, orders_info]
            ) else 'partial_success'
        }
        
        return final_result
        
    except Exception as e:
        logger.error(f"❌ Erreur traitement ultra complet: {e}")
        return None

def check_server_status():
    """Vérifie si le serveur Django est accessible"""
    try:
        response = requests.get(f"{SERVER_URL}/admin/", timeout=5)
        print("✅ Serveur Django accessible")
        return True
    except:
        logger.error("❌ Serveur Django inaccessible")
        logger.error(f"   Vérifiez que le serveur tourne sur {SERVER_URL}")
        return False

def main():
    print("=" * 90)
    print("🏥 SCRIPT PHARMACIE MOUYSSET V2 - COMPLET (PRODUITS + VENTES + ACHATS)")
    print("=" * 90)
    
    print(f"🔧 Configuration:")
    print(f"   SERVER_URL: {SERVER_URL}")
    print(f"   Pharmacie: {PHARMACY_NAME}")
    print(f"   ID National: {PHARMACY_ID_NAT}")
    print(f"   Ventes août: {AUGUST_SALES_PERIOD['dt1']} -> {AUGUST_SALES_PERIOD['dt2']}")
    print(f"   Achats avril-août: {ORDERS_PERIOD['dt1']} -> {ORDERS_PERIOD['dt2']}")
    
    # Vérifier la connexion serveur
    print("\n🔍 Vérification du serveur...")
    if not check_server_status():
        logger.error("💥 ARRÊT: Serveur inaccessible")
        return
    
    print("\nOptions disponibles:")
    print("1. Créer uniquement la pharmacie")
    print("2. Récupérer uniquement les produits (pharmacie doit exister)")
    print("3. Récupérer uniquement les ventes d'août (pharmacie + produits doivent exister)")
    print("4. Récupérer uniquement les achats avril-août (pharmacie doit exister)")
    print("5. Traitement pharmacie + produits")
    print("6. 🌟 TRAITEMENT ULTRA COMPLET: pharmacie + produits + ventes + achats")
    
    choice = input("\nChoisissez une option (1/2/3/4/5/6): ").strip()
    
    start_time = print("🚀 Début du traitement...")
    
    if choice == "1":
        print("🎯 Option 1: Création pharmacie uniquement")
        result = create_pharmacy_only(PHARMACY_NAME, PHARMACY_ID_NAT)
        success = result is not None
        
    elif choice == "2":
        print("🎯 Option 2: Récupération produits uniquement")
        result = fetch_products(PHARMACY_ID_NAT)
        success = result is not None
        
    elif choice == "3":
        print("🎯 Option 3: Récupération ventes août uniquement")
        logger.warning("⚠️ Requires existing pharmacy with products!")
        result = fetch_sales(PHARMACY_ID_NAT, AUGUST_SALES_PERIOD['dt1'], AUGUST_SALES_PERIOD['dt2'])
        success = result is not None
        
    elif choice == "4":
        print("🎯 Option 4: Récupération achats avril-août uniquement")
        logger.warning("⚠️ Requires existing pharmacy!")
        result = fetch_orders(PHARMACY_ID_NAT, ORDERS_PERIOD['dt1'], ORDERS_PERIOD['dt2'])
        success = result is not None
        
    elif choice == "5":
        print("🎯 Option 5: Traitement pharmacie + produits")
        result = create_and_fetch_products(PHARMACY_NAME, PHARMACY_ID_NAT)
        success = result is not None
        
    elif choice == "6":
        print("🎯 Option 6: TRAITEMENT ULTRA COMPLET (pharmacie + produits + ventes + achats)")
        logger.warning("⚠️ Traitement le plus long - peut prendre 10-15 minutes")
        confirm = input("Continuer avec le traitement ultra complet ? (o/N): ").strip().lower()
        
        if confirm in ['o', 'oui', 'y', 'yes']:
            result = create_products_sales_and_orders_complete(
                PHARMACY_NAME, 
                PHARMACY_ID_NAT, 
                AUGUST_SALES_PERIOD, 
                ORDERS_PERIOD
            )
            success = result is not None
        else:
            print("Traitement annulé")
            return
        
    else:
        logger.error("❌ Option invalide")
        return
    
    print("\n" + "=" * 90)
    if success:
        print("🎉 SCRIPT TERMINÉ AVEC SUCCÈS")
        
        if choice == "6" and result:
            # Résumé final pour le traitement ultra complet
            pharmacy_info = result.get('pharmacy', {})
            products_info = result.get('products', {})
            sales_info = result.get('sales', {})
            orders_info = result.get('orders', {})
            
            print("📄 RÉSUMÉ FINAL ULTRA COMPLET:")
            print(f"   Pharmacie: {pharmacy_info.get('status', 'unknown')}")
            print(f"   Produits: {products_info.get('status', 'unknown')}")
            print(f"   Ventes: {sales_info.get('status', 'unknown')}")
            print(f"   Achats: {orders_info.get('status', 'unknown')}")
            print(f"   Status global: {result.get('overall_status', 'unknown')}")
            
            # Statistiques détaillées
            products_stats = products_info.get('stats', {})
            sales_stats = sales_info.get('stats', {})
            orders_stats = orders_info.get('stats', {})
            
            if products_stats or sales_stats or orders_stats:
                print(f"   📊 Statistiques détaillées:")
                if products_stats:
                    print(f"      Produits traités: {products_stats.get('products_processed', 0)}")
                    print(f"      Snapshots créés: {products_stats.get('snapshots_created', 0)}")
                if sales_stats:
                    print(f"      Ventes traitées: {sales_stats.get('sales_processed', 'N/A')}")
                    print(f"      TVA mises à jour: {sales_stats.get('tva_updates', 0)}")
                if orders_stats:
                    print(f"      Fournisseurs traités: {orders_stats.get('suppliers_processed', 0)}")
                    print(f"      Commandes traitées: {orders_stats.get('orders_processed', 0)}")
                    
    else:
        logger.error("💥 SCRIPT TERMINÉ AVEC ERREUR")
        logger.error("   Consultez les logs ci-dessus pour plus de détails")
    
    print("=" * 90)

if __name__ == "__main__":
    main()