#!/usr/bin/env python3
"""
Debug profond pour comprendre pourquoi le service traite 0 produits
"""

import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

def test_with_logs():
    """Test avec tentative de récupération des logs"""
    
    print("🔍 Test avec debug des paramètres")
    print("-" * 50)
    
    # Test direct de l'API avec nos paramètres exacts
    base_url = "https://www.pharmanuage.fr/data-api/v2"
    username = os.environ.get('APOTHICAL_USERNAME')
    password = os.environ.get('APOTHICAL_PASSWORD')
    finess = "712006733"
    
    # Authentification
    auth_response = requests.post(f"{base_url}/auth", json={
        "username": username,
        "password": password
    })
    
    if auth_response.status_code != 200:
        print("❌ Authentification échouée")
        return
    
    token = auth_response.json()['token']
    headers = {"Authorization": f"Bearer {token}"}
    
    # Test avec les MÊMES paramètres que notre service
    print("🧪 Test avec paramètres service Django:")
    
    # Page 0, size 100 (comme notre service)
    params = {"page": 0, "size": 100}
    products_response = requests.get(
        f"{base_url}/{finess}/products",
        headers=headers,
        params=params,
        timeout=60
    )
    
    print(f"URL: {base_url}/{finess}/products")
    print(f"Params: {params}")
    print(f"Status: {products_response.status_code}")
    
    if products_response.status_code == 200:
        data = products_response.json()
        
        print(f"Type réponse: {type(data)}")
        print(f"Clés: {list(data.keys()) if isinstance(data, dict) else 'N/A'}")
        
        if '_embedded' in data:
            embedded = data['_embedded']
            print(f"_embedded clés: {list(embedded.keys())}")
            
            if 'products' in embedded:
                products = embedded['products']
                print(f"✅ {len(products)} produits dans _embedded.products")
                
                if products:
                    first = products[0]
                    print(f"Premier produit ID: {first.get('productId')}")
                    print(f"Premier produit nom: {first.get('description', 'N/A')}")
                    print(f"Premier produit stock: {first.get('stockQuantity', 'N/A')}")
                    
                    # Test des filtres qui pourraient éliminer les produits
                    print("\n🔍 Analyse des données qui pourraient être filtrées:")
                    
                    for i, prod in enumerate(products[:3]):  # Examiner les 3 premiers
                        pid = prod.get('productId')
                        stock = prod.get('stockQuantity', 0)
                        official_code = prod.get('officialProductCode') or prod.get('ean13')
                        
                        print(f"  Produit {i+1}:")
                        print(f"    ID: {pid} (valide: {pid and int(pid) >= 0})")
                        print(f"    Stock: {stock}")
                        print(f"    Code officiel: {official_code}")
                        
                        # Vérifier si le produit passerait nos filtres
                        would_pass = True
                        if not pid or int(pid) < 0:
                            would_pass = False
                            print(f"    ❌ Serait filtré: ID invalide")
                        
                        if would_pass:
                            print(f"    ✅ Passerait nos filtres")
            else:
                print("❌ Pas de clé 'products' dans _embedded")
        else:
            print("❌ Pas de clé '_embedded' dans la réponse")
    else:
        print(f"❌ Erreur: {products_response.status_code}")
        print(f"Réponse: {products_response.text[:500]}")


def test_django_service_with_debug():
    """Test du service Django avec plus de debug"""
    
    print("\n🔍 Test service Django avec debug")
    print("-" * 50)
    
    # Test en ajoutant des paramètres de debug
    url = "https://api.phardev.fr/apothical/create/products"
    headers = {
        'Pharmacy-Finess': '712006733',
        'Content-Type': 'application/json'
    }
    
    # Body avec flag de debug si disponible
    body = {"debug": True}
    
    response = requests.post(url, json=body, headers=headers, timeout=60)
    
    print(f"URL: {url}")
    print(f"Headers: {headers}")
    print(f"Body: {body}")
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"Réponse complète: {json.dumps(data, indent=2)}")
    else:
        print(f"Erreur: {response.text}")


def test_environment_variables():
    """Vérifier les variables d'environnement"""
    
    print("\n🔍 Vérification variables d'environnement")
    print("-" * 50)
    
    vars_to_check = [
        'APOTHICAL_USERNAME',
        'APOTHICAL_PASSWORD'
    ]
    
    for var in vars_to_check:
        value = os.environ.get(var)
        if value:
            if 'PASSWORD' in var:
                print(f"✅ {var}: {'*' * len(value)}")
            else:
                print(f"✅ {var}: {value}")
        else:
            print(f"❌ {var}: NON DÉFINI")


def test_pagination_theory():
    """Test théorique de la pagination"""
    
    print("\n🔍 Test théorique pagination")
    print("-" * 50)
    
    # Simuler ce que fait notre code
    print("Simulation du code Django:")
    print("1. fetch_paginated_data('products', '712006733')")
    print("2. Boucle pagination avec page=0, size=100")
    print("3. Extraction data['_embedded']['products']")
    print("4. Traitement de chaque produit")
    
    print("\nPoints de défaillance possibles:")
    print("❓ Variables d'environnement manquantes sur le serveur")
    print("❓ Timeout de l'API sur le serveur (différent du local)")
    print("❓ Logs Django cachent les vraies erreurs")
    print("❓ Cache/proxy qui interfère")


def main():
    print("🐛 DEBUG PROFOND - Service Apothical")
    print("=" * 60)
    
    # Vérifications
    test_environment_variables()
    test_with_logs()
    test_django_service_with_debug()
    test_pagination_theory()
    
    print("\n💡 HYPOTHÈSES:")
    print("1. 🔐 Variables d'env pas définies sur le serveur de prod")
    print("2. ⏱️ Timeout API différent en prod") 
    print("3. 🚫 Firewall/proxy qui bloque l'API externe")
    print("4. 📝 Logs Django pas visibles (erreurs silencieuses)")
    
    print("\n🔧 SOLUTIONS À TESTER:")
    print("1. Vérifier les variables d'env sur le serveur")
    print("2. Tester en local avec python manage.py runserver")
    print("3. Ajouter des logs debug dans le service")
    print("4. Vérifier les timeouts réseau")


if __name__ == "__main__":
    main()