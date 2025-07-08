#!/usr/bin/env python3
"""
Test de connexion à l'API Apothical (Smart-RX PharmaCloud)
"""

import requests
import json
from datetime import datetime
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

class ApothicalAPITest:
    def __init__(self):
        self.base_url = "https://www.pharmanuage.fr/data-api/v2"
        self.username = os.environ.get('APOTHICAL_USERNAME')
        self.password = os.environ.get('APOTHICAL_PASSWORD')
        self.token = None
    
    def authenticate(self):
        """Authentification et récupération du token"""
        if not self.username or not self.password:
            print("❌ Erreur: APOTHICAL_USERNAME et APOTHICAL_PASSWORD doivent être définis dans .env")
            return False
            
        auth_url = f"{self.base_url}/auth"
        payload = {
            "username": self.username,
            "password": self.password
        }
        
        try:
            print(f"🔐 Tentative d'authentification sur {auth_url}")
            response = requests.post(auth_url, json=payload, timeout=10)
            
            print(f"Status Code: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                if 'token' in data:
                    self.token = data['token']
                    print(f"✅ Authentification réussie")
                    print(f"Token: {self.token[:20]}..." if len(self.token) > 20 else self.token)
                    return True
                else:
                    print(f"❌ Token non trouvé dans la réponse: {data}")
                    return False
            else:
                print(f"❌ Échec authentification: {response.status_code}")
                try:
                    print(f"Réponse: {response.json()}")
                except:
                    print(f"Réponse brute: {response.text}")
                return False
                
        except requests.exceptions.Timeout:
            print("❌ Timeout lors de l'authentification")
            return False
        except requests.exceptions.RequestException as e:
            print(f"❌ Erreur de connexion: {e}")
            return False
    
    def discover_valid_finess(self):
        """Tente de découvrir les codes FINESS valides"""
        if not self.token:
            return []
            
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        # Codes à tester basés sur l'email
        test_codes = [
            # FINESS possibles (sans le 'o')
            "062037692", "062050992", "712006733",
            # FINESS originaux
            "o62037692", "o62050992", "712006733", 
            # CIP codes
            "2001439", "2060866", "2007123",
            # FINESS avec zéros supplémentaires
            "0062037692", "0062050992", "0712006733",
            # Autres formats possibles
            "62037692", "62050992"
        ]
        
        valid_codes = []
        
        print("\n🔍 Découverte des codes FINESS valides...")
        print("-" * 40)
        
        for code in test_codes:
            url = f"{self.base_url}/{code}/products"
            params = {"size": 1, "page": 0}
            
            try:
                response = requests.get(url, headers=headers, params=params, timeout=5)
                
                if response.status_code == 200:
                    data = response.json()
                    count = len(data) if isinstance(data, list) else "N/A"
                    print(f"✅ Code valide: {code} ({count} produits)")
                    valid_codes.append(code)
                elif response.status_code == 403:
                    print(f"🚫 Code valide mais accès refusé: {code}")
                elif response.status_code == 500:
                    error_msg = ""
                    try:
                        error_data = response.json()
                        error_msg = error_data.get('message', '')
                    except:
                        pass
                    if 'finess' in error_msg.lower():
                        print(f"❌ Code invalide: {code}")
                    else:
                        print(f"⚠️  Code {code}: Erreur 500 - {error_msg}")
                elif response.status_code == 404:
                    print(f"❌ Code introuvable: {code}")
                else:
                    print(f"⚠️  Code {code}: Status {response.status_code}")
                    
            except Exception as e:
                print(f"❌ Erreur test {code}: {e}")
        
        return valid_codes
    
    def test_endpoints(self, finess):
        """Test des différents endpoints disponibles"""
        if not self.token:
            return False
            
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        endpoints = [
            ("products", "produits"),
            ("orders", "commandes"), 
            ("sales", "ventes")
        ]
        
        results = {}
        
        print(f"\n🔧 Test des endpoints pour FINESS: {finess}")
        print("-" * 40)
        
        for endpoint, description in endpoints:
            url = f"{self.base_url}/{finess}/{endpoint}"
            params = {"size": 3, "page": 0}  # Plus de données pour mieux tester
            
            try:
                print(f"🔍 Test {description}...")
                response = requests.get(url, headers=headers, params=params, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    count = len(data) if isinstance(data, list) else "N/A"
                    print(f"✅ {description}: OK ({count} éléments)")
                    
                    # Afficher un exemple de données
                    if isinstance(data, list) and data:
                        first_item = data[0]
                        if endpoint == "products":
                            prod_id = first_item.get('productId', 'N/A')
                            description_text = first_item.get('description', 'N/A')[:50]
                            print(f"   Exemple: ID={prod_id}, Desc={description_text}...")
                        elif endpoint == "orders":
                            order_num = first_item.get('orderNumber', 'N/A')
                            status = first_item.get('orderStatus', 'N/A')
                            print(f"   Exemple: Commande #{order_num}, Status={status}")
                        elif endpoint == "sales":
                            sale_num = first_item.get('saleNumber', 'N/A')
                            sale_date = first_item.get('date', 'N/A')
                            print(f"   Exemple: Vente #{sale_num}, Date={sale_date}")
                    
                    results[endpoint] = True
                else:
                    print(f"❌ {description}: {response.status_code}")
                    try:
                        error = response.json()
                        print(f"   Erreur: {error.get('message', 'N/A')}")
                    except:
                        pass
                    results[endpoint] = False
                    
            except Exception as e:
                print(f"❌ {description}: Erreur {e}")
                results[endpoint] = False
        
        return results
    
    def run_full_test(self):
        """Exécute tous les tests"""
        print("🚀 Début des tests API Apothical")
        print("=" * 60)
        
        # Test d'authentification
        if not self.authenticate():
            print("\n❌ Tests interrompus - Échec authentification")
            return
        
        # Découverte des codes FINESS valides
        valid_codes = self.discover_valid_finess()
        
        if valid_codes:
            print(f"\n🎯 {len(valid_codes)} code(s) FINESS valide(s) trouvé(s): {valid_codes}")
            
            # Test avec le premier code valide
            first_valid = valid_codes[0]
            endpoint_results = self.test_endpoints(first_valid)
            
            # Résumé des endpoints
            working_endpoints = sum(endpoint_results.values())
            total_endpoints = len(endpoint_results)
            print(f"\n📊 Endpoints fonctionnels: {working_endpoints}/{total_endpoints}")
            
        else:
            print("\n❌ Aucun code FINESS valide trouvé")
            
        print("\n" + "=" * 60)
        print("📋 RÉSUMÉ FINAL")
        print("-" * 20)
        
        if valid_codes:
            print("✅ API Apothical: FONCTIONNELLE")
            print(f"✅ Codes FINESS valides: {valid_codes}")
            print("✅ Prêt pour l'intégration complète")
            
            print("\n📝 Prochaines étapes:")
            print("1. Créer le service data/services/apothical.py")
            print("2. Ajouter les vues dans data/views.py") 
            print("3. Configurer les URLs")
            print("4. Créer la Lambda de récupération")
        else:
            print("❌ API non accessible avec les codes fournis")
            print("📧 Contacter Cédric FONTERAY pour vérifier les codes FINESS")
            print("   Email: Cedric.FONTERAY@smart-rx.com")


if __name__ == "__main__":
    # Informations importantes pour la configuration
    print("📋 Configuration requise dans .env:")
    print("APOTHICAL_USERNAME=votre_username")
    print("APOTHICAL_PASSWORD=votre_password")
    print()
    
    # Lancement des tests
    tester = ApothicalAPITest()
    tester.run_full_test()