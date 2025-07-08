#!/usr/bin/env python3
"""
Test d'intégration complète Apothical
"""

import os
import requests
import json
from datetime import datetime

# Configuration
SERVER_URL = "https://api.phardev.fr"  # ou "http://localhost:8000" pour le dev local
FINESS_CODE = "712006733"  # Pharmacie Puig Léveilé


def test_endpoint(endpoint_name, description):
    """Test un endpoint spécifique"""
    url = f"{SERVER_URL}/apothical/create/{endpoint_name}"
    headers = {
        'Pharmacy-Finess': FINESS_CODE,
        'Content-Type': 'application/json'
    }
    
    try:
        print(f"🔄 Test {description}...")
        
        # Appel à l'endpoint
        response = requests.post(url, json={}, headers=headers, timeout=120)
        
        print(f"   Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"   ✅ {data.get('message', 'OK')}")
            
            # Affichage des statistiques
            stats = []
            if 'products_count' in data:
                stats.append(f"📦 {data['products_count']} produits")
            if 'snapshots_count' in data:
                stats.append(f"📊 {data['snapshots_count']} snapshots")
            if 'orders_count' in data:
                stats.append(f"📋 {data['orders_count']} commandes")
            if 'suppliers_count' in data:
                stats.append(f"🏢 {data['suppliers_count']} fournisseurs")
            if 'product_orders_count' in data:
                stats.append(f"🔗 {data['product_orders_count']} liens prod-cmd")
                
            if stats:
                print(f"   📈 {' | '.join(stats)}")
            
            return True
            
        else:
            print(f"   ❌ Erreur {response.status_code}")
            try:
                error_data = response.json()
                print(f"   💬 {error_data.get('message', 'Erreur inconnue')}")
            except:
                print(f"   📄 {response.text}")
            return False
            
    except requests.exceptions.Timeout:
        print(f"   ⏱️ Timeout après 2 minutes")
        return False
    except requests.exceptions.ConnectionError:
        print(f"   🚫 Impossible de se connecter à {SERVER_URL}")
        return False
    except Exception as e:
        print(f"   ❌ Erreur: {e}")
        return False


def test_api_availability():
    """Test la disponibilité de l'API"""
    try:
        response = requests.get(f"{SERVER_URL}/admin/", timeout=10)
        if response.status_code in [200, 301, 302]:
            print("✅ API Django accessible")
            return True
        else:
            print(f"⚠️ API répond avec status {response.status_code}")
            return True  # On continue quand même
    except:
        print("❌ API Django non accessible")
        return False


def main():
    """Test complet de l'intégration Apothical"""
    print("🧪 Test d'intégration Apothical")
    print("=" * 60)
    print(f"🌐 Serveur: {SERVER_URL}")
    print(f"🏥 FINESS: {FINESS_CODE}")
    print(f"⏰ Heure: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Test de disponibilité de l'API
    if not test_api_availability():
        print("\n❌ Tests interrompus - API non accessible")
        return
    
    # Tests des endpoints
    endpoints = [
        ("products", "Produits et stocks"),
        ("orders", "Commandes"),
        ("sales", "Ventes")
    ]
    
    results = []
    
    for endpoint, description in endpoints:
        success = test_endpoint(endpoint, description)
        results.append((endpoint, success))
        print()  # Ligne vide entre les tests
    
    # Résumé final
    print("=" * 60)
    print("📋 RÉSUMÉ DES TESTS")
    print("-" * 30)
    
    success_count = 0
    for endpoint, success in results:
        status = "✅" if success else "❌"
        print(f"{status} {endpoint.capitalize()}")
        if success:
            success_count += 1
    
    print()
    print(f"📊 Score: {success_count}/{len(results)} endpoints fonctionnels")
    
    if success_count == len(results):
        print("🎉 Intégration Apothical: PARFAITE")
        print("✅ Prêt pour la mise en production")
    elif success_count > 0:
        print("⚠️ Intégration Apothical: PARTIELLE")
        print("🔧 Certains endpoints nécessitent des ajustements")
    else:
        print("❌ Intégration Apothical: ÉCHEC")
        print("🚨 Vérifier la configuration et les logs")
    
    print()
    print("📝 Prochaines étapes:")
    if success_count > 0:
        print("1. ✅ Configurer la Lambda de synchronisation automatique")
        print("2. ✅ Programmer l'exécution quotidienne")
        print("3. ✅ Demander l'activation des 2 autres pharmacies")
    else:
        print("1. 🔧 Déboguer les erreurs de connexion")
        print("2. 🔍 Vérifier les logs Django")
        print("3. 📧 Contacter le support si nécessaire")


if __name__ == "__main__":
    main()