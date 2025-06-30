#!/usr/bin/env python3
"""
Script pour extraire et sauvegarder la réponse API brute pour un produit spécifique
"""

import json
import requests
from datetime import datetime

# Configuration API
API_URL = "YXBvdGhpY2Fs"
API_PASSWORD = "cGFzczE"
PHARMACY_ID = "832011373"
BASE_URL = "https://grpstat.winpharma.com/ApiWp"

# Produit à chercher
TARGET_PRODUCT_CODE = "3662361002146"

def get_full_api_response():
    """
    Récupère la réponse complète de l'API
    """
    print(f"📡 Récupération de la réponse API complète...")
    
    url = f"{BASE_URL}/{API_URL}/produits"
    params = {
        'password': API_PASSWORD,
        'Idnats': PHARMACY_ID
    }
    
    try:
        response = requests.get(url, params=params, timeout=120)
        
        if response.status_code != 200:
            print(f"❌ Erreur API: {response.status_code} - {response.text}")
            return None
            
        data = response.json()
        print(f"✅ Réponse API reçue avec succès")
        return data
        
    except Exception as e:
        print(f"💥 Erreur: {e}")
        return None

def find_product_in_response(data, target_code):
    """
    Trouve le produit spécifique dans la réponse
    """
    if not data or len(data) == 0:
        return None, None
    
    pharmacy_data = data[0]
    products = pharmacy_data.get("produits", [])
    
    print(f"🔍 Recherche du produit {target_code} parmi {len(products)} produits...")
    
    # Rechercher par Code13Ref
    for index, product in enumerate(products):
        if product.get('Code13Ref') == target_code:
            return product, index
    
    # Rechercher par ProdId
    for index, product in enumerate(products):
        if str(product.get('ProdId')) == target_code:
            return product, index
    
    # Rechercher dans ExtraCodes
    for index, product in enumerate(products):
        extra_codes = product.get('ExtraCodes', [])
        if isinstance(extra_codes, list):
            for extra_code in extra_codes:
                if isinstance(extra_code, dict) and extra_code.get('code') == target_code:
                    return product, index
    
    return None, None

def save_responses_to_files(full_response, target_product, product_index):
    """
    Sauvegarde les réponses dans des fichiers JSON
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 1. Sauvegarder la réponse API complète (structure seulement, pas tous les produits)
    if full_response:
        pharmacy_data = full_response[0]
        structure_sample = {
            "timestamp": timestamp,
            "api_url": f"{BASE_URL}/{API_URL}/produits",
            "pharmacy_id": PHARMACY_ID,
            "response_structure": {
                "type": str(type(full_response)),
                "length": len(full_response),
                "pharmacy_data_keys": list(pharmacy_data.keys()),
                "total_products": len(pharmacy_data.get("produits", [])),
                "sample_product_keys": list(pharmacy_data.get("produits", [{}])[0].keys()) if pharmacy_data.get("produits") else []
            }
        }
        
        filename_structure = f"api_structure_{timestamp}.json"
        with open(filename_structure, 'w', encoding='utf-8') as f:
            json.dump(structure_sample, f, indent=2, ensure_ascii=False)
        print(f"📁 Structure API sauvée: {filename_structure}")
    
    # 2. Sauvegarder 10 produits d'exemple autour du produit trouvé
    if full_response and target_product and product_index is not None:
        pharmacy_data = full_response[0]
        products = pharmacy_data.get("produits", [])
        
        # Prendre 5 produits avant et 5 après (si possible)
        start_idx = max(0, product_index - 5)
        end_idx = min(len(products), product_index + 6)
        
        sample_products = {
            "timestamp": timestamp,
            "target_product_code": TARGET_PRODUCT_CODE,
            "target_product_index": product_index,
            "sample_range": f"{start_idx} to {end_idx-1}",
            "products": products[start_idx:end_idx]
        }
        
        filename_sample = f"api_sample_products_{timestamp}.json"
        with open(filename_sample, 'w', encoding='utf-8') as f:
            json.dump(sample_products, f, indent=2, ensure_ascii=False)
        print(f"📁 Échantillon de produits sauvé: {filename_sample}")
    
    # 3. Sauvegarder le produit spécifique trouvé
    if target_product:
        product_data = {
            "timestamp": timestamp,
            "search_code": TARGET_PRODUCT_CODE,
            "product_index": product_index,
            "found_product": target_product
        }
        
        filename_target = f"target_product_{TARGET_PRODUCT_CODE}_{timestamp}.json"
        with open(filename_target, 'w', encoding='utf-8') as f:
            json.dump(product_data, f, indent=2, ensure_ascii=False)
        print(f"📁 Produit cible sauvé: {filename_target}")
    
    # 4. Sauvegarder un échantillon de produits avec prix pour comparaison
    if full_response:
        pharmacy_data = full_response[0]
        products = pharmacy_data.get("produits", [])
        
        # Trouver quelques produits avec des prix non-nuls
        products_with_prices = []
        products_without_prices = []
        
        for product in products[:100]:  # Échantillon des 100 premiers
            prix_ttc = product.get('PrixTTC', 0)
            prix_mp = product.get('PrixMP', 0)
            
            if prix_ttc > 0 or prix_mp > 0:
                products_with_prices.append(product)
            else:
                products_without_prices.append(product)
            
            # Limiter les échantillons
            if len(products_with_prices) >= 10 and len(products_without_prices) >= 5:
                break
        
        price_analysis = {
            "timestamp": timestamp,
            "analysis": {
                "total_products_analyzed": min(100, len(products)),
                "with_prices_count": len(products_with_prices),
                "without_prices_count": len(products_without_prices)
            },
            "products_with_prices": products_with_prices[:10],
            "products_without_prices": products_without_prices[:5]
        }
        
        filename_prices = f"price_analysis_{timestamp}.json"
        with open(filename_prices, 'w', encoding='utf-8') as f:
            json.dump(price_analysis, f, indent=2, ensure_ascii=False)
        print(f"📁 Analyse des prix sauvée: {filename_prices}")

def create_summary_report(target_product, product_index, total_products):
    """
    Crée un rapport de synthèse
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if target_product:
        summary = {
            "timestamp": timestamp,
            "search_summary": {
                "target_code": TARGET_PRODUCT_CODE,
                "status": "FOUND",
                "product_index": product_index,
                "total_products": total_products
            },
            "product_details": {
                "prod_id": target_product.get('ProdId'),
                "name": target_product.get('Nom'),
                "code13_ref": target_product.get('Code13Ref'),
                "stock": target_product.get('Stock'),
                "prix_ttc": target_product.get('PrixTTC'),
                "prix_mp": target_product.get('PrixMP'),
                "extra_codes": target_product.get('ExtraCodes', [])
            },
            "price_analysis": {
                "has_prix_ttc": target_product.get('PrixTTC', 0) > 0,
                "has_prix_mp": target_product.get('PrixMP', 0) > 0,
                "prix_ttc_value": target_product.get('PrixTTC', 0),
                "prix_mp_value": target_product.get('PrixMP', 0)
            }
        }
    else:
        summary = {
            "timestamp": timestamp,
            "search_summary": {
                "target_code": TARGET_PRODUCT_CODE,
                "status": "NOT_FOUND",
                "total_products": total_products
            },
            "recommendation": "Le produit n'a pas été trouvé. Vérifiez le code ou consultez les fichiers d'échantillons."
        }
    
    filename_summary = f"search_summary_{TARGET_PRODUCT_CODE}_{timestamp}.json"
    with open(filename_summary, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"📁 Rapport de synthèse sauvé: {filename_summary}")

def main():
    """
    Fonction principale
    """
    print(f"🎯 EXTRACTION RÉPONSE API - PRODUIT {TARGET_PRODUCT_CODE}")
    print("=" * 60)
    
    # 1. Récupérer la réponse API complète
    full_response = get_full_api_response()
    if not full_response:
        print("❌ Impossible de récupérer la réponse API")
        return
    
    # 2. Chercher le produit spécifique
    target_product, product_index = find_product_in_response(full_response, TARGET_PRODUCT_CODE)
    
    total_products = len(full_response[0].get("produits", []))
    
    if target_product:
        print(f"✅ Produit trouvé à l'index {product_index}")
        print(f"   ID: {target_product.get('ProdId')}")
        print(f"   Nom: {target_product.get('Nom')}")
        print(f"   Prix TTC: {target_product.get('PrixTTC')}")
        print(f"   Prix MP: {target_product.get('PrixMP')}")
    else:
        print(f"❌ Produit {TARGET_PRODUCT_CODE} non trouvé")
    
    # 3. Sauvegarder tous les fichiers
    print(f"\n💾 Sauvegarde des fichiers...")
    save_responses_to_files(full_response, target_product, product_index)
    create_summary_report(target_product, product_index, total_products)
    
    print(f"\n🎉 EXTRACTION TERMINÉE")
    print("=" * 60)
    print(f"📁 Fichiers générés dans le répertoire courant:")
    print(f"   • api_structure_*.json - Structure de l'API")
    print(f"   • api_sample_products_*.json - Échantillon de produits")
    print(f"   • target_product_*.json - Produit recherché")
    print(f"   • price_analysis_*.json - Analyse des prix")
    print(f"   • search_summary_*.json - Rapport de synthèse")

if __name__ == "__main__":
    main()