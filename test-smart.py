#!/usr/bin/env python3
"""
Debug pour vérifier si le service Apothical a bien le fix _embedded
"""

import requests
import os
from dotenv import load_dotenv

load_dotenv()

def test_api_directly():
    """Test direct de l'API Apothical pour comparer"""
    print("🔍 Test direct API Apothical")
    print("-" * 40)
    
    base_url = "https://www.pharmanuage.fr/data-api/v2"
    username = os.environ.get('APOTHICAL_USERNAME')
    password = os.environ.get('APOTHICAL_PASSWORD')
    
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
    
    # Test products direct
    products_response = requests.get(
        f"{base_url}/712006733/products",
        headers=headers,
        params={"page": 0, "size": 5}
    )
    
    if products_response.status_code == 200:
        data = products_response.json()
        if '_embedded' in data and 'products' in data['_embedded']:
            products = data['_embedded']['products']
            print(f"✅ API directe: {len(products)} produits trouvés")
            
            if products:
                first_product = products[0]
                print(f"   Premier produit: {first_product.get('description', 'N/A')}")
                print(f"   Product ID: {first_product.get('productId', 'N/A')}")
                print(f"   Stock: {first_product.get('stockQuantity', 'N/A')}")
        else:
            print("❌ Format _embedded non trouvé dans la réponse")
    else:
        print(f"❌ Erreur API: {products_response.status_code}")


def test_service_endpoint():
    """Test de notre endpoint Django"""
    print("\n🔍 Test endpoint Django")
    print("-" * 40)
    
    url = "https://api.phardev.fr/apothical/create/products"
    headers = {'Pharmacy-Finess': '712006733'}
    
    response = requests.post(url, json={}, headers=headers, timeout=60)
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"Message: {data.get('message')}")
        print(f"Products: {data.get('products_count', 'N/A')}")
        print(f"Snapshots: {data.get('snapshots_count', 'N/A')}")
    else:
        print(f"Erreur: {response.text}")


def check_logs_github():
    """Instructions pour vérifier les logs GitHub"""
    print("\n🔍 Vérifications à faire")
    print("-" * 40)
    print("1. 📋 GitHub Actions:")
    print("   - Va sur GitHub → Actions")
    print("   - Vérifie que le dernier commit a été déployé avec succès")
    print("   - Regarde s'il y a des erreurs de build")
    
    print("\n2. 🐛 Debug local:")
    print("   - Teste le service en local d'abord")
    print("   - python manage.py runserver")
    print("   - Teste sur http://localhost:8000")
    
    print("\n3. 📊 Comparaison:")
    print("   - L'API directe devrait retourner des données")
    print("   - Notre service devrait traiter ces mêmes données")


def main():
    print("🐛 Debug déploiement service Apothical")
    print("=" * 50)
    
    # Test API directe
    test_api_directly()
    
    # Test service Django
    test_service_endpoint()
    
    # Instructions
    check_logs_github()
    
    print("\n💡 Solutions possibles:")
    print("1. 🔄 Le déploiement prend du temps → attendre 2-3 min")
    print("2. 🚫 Erreur de build → vérifier GitHub Actions")
    print("3. 🐛 Bug dans le code → tester en local")
    print("4. 🔧 Cache Docker → forcer un rebuild")


if __name__ == "__main__":
    main()