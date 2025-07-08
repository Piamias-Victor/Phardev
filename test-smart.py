#!/usr/bin/env python3
"""
Debug des réponses API Apothical pour comprendre pourquoi 0 données
"""

import requests
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

class ApothicalDebugger:
    def __init__(self):
        self.base_url = "https://www.pharmanuage.fr/data-api/v2"
        self.username = os.environ.get('APOTHICAL_USERNAME')
        self.password = os.environ.get('APOTHICAL_PASSWORD')
        self.token = None
        self.finess = "712006733"
    
    def authenticate(self):
        """Authentification"""
        auth_url = f"{self.base_url}/auth"
        payload = {
            "username": self.username,
            "password": self.password
        }
        
        response = requests.post(auth_url, json=payload, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            self.token = data['token']
            print(f"✅ Token récupéré: {self.token[:20]}...")
            return True
        else:
            print(f"❌ Authentification échouée: {response.status_code}")
            return False
    
    def debug_endpoint(self, endpoint, params=None, description=""):
        """Debug d'un endpoint spécifique"""
        if not self.token:
            print("❌ Pas de token")
            return None
        
        url = f"{self.base_url}/{self.finess}/{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        print(f"\n🔍 Debug {endpoint} - {description}")
        print("-" * 60)
        print(f"🌐 URL: {url}")
        print(f"📋 Params: {params}")
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=60)
            
            print(f"📊 Status: {response.status_code}")
            print(f"📏 Content-Length: {response.headers.get('content-length', 'N/A')}")
            print(f"📝 Content-Type: {response.headers.get('content-type', 'N/A')}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    
                    if isinstance(data, list):
                        print(f"📦 Type: Liste avec {len(data)} éléments")
                        
                        if len(data) > 0:
                            print(f"🔍 Premier élément:")
                            first_item = data[0]
                            
                            # Afficher les clés principales
                            if isinstance(first_item, dict):
                                keys = list(first_item.keys())[:10]  # Max 10 clés
                                print(f"   Clés: {keys}")
                                
                                # Afficher quelques valeurs importantes
                                important_keys = ['productId', 'description', 'stockQuantity', 'orderNumber', 'saleNumber']
                                for key in important_keys:
                                    if key in first_item:
                                        value = first_item[key]
                                        print(f"   {key}: {value}")
                            
                            # Afficher le JSON complet du premier élément (limité)
                            print(f"🔍 Premier élément (JSON):")
                            print(json.dumps(first_item, indent=2, ensure_ascii=False)[:1000] + "...")
                        else:
                            print("📭 Liste vide")
                            
                    elif isinstance(data, dict):
                        print(f"📦 Type: Dictionnaire")
                        print(f"   Clés: {list(data.keys())}")
                        print(f"🔍 Contenu:")
                        print(json.dumps(data, indent=2, ensure_ascii=False)[:1000] + "...")
                    else:
                        print(f"📦 Type: {type(data)}")
                        print(f"🔍 Contenu: {str(data)[:500]}...")
                    
                    return data
                    
                except json.JSONDecodeError:
                    print("❌ Réponse non-JSON")
                    print(f"🔍 Contenu brut: {response.text[:500]}...")
                    return None
            else:
                print(f"❌ Erreur {response.status_code}")
                try:
                    error_data = response.json()
                    print(f"🔍 Erreur: {json.dumps(error_data, indent=2)}")
                except:
                    print(f"🔍 Réponse brute: {response.text}")
                return None
                
        except Exception as e:
            print(f"💥 Exception: {e}")
            return None
    
    def run_full_debug(self):
        """Debug complet de tous les endpoints"""
        print("🐛 Debug complet API Apothical")
        print("=" * 70)
        print(f"🏥 FINESS: {self.finess}")
        print(f"⏰ Timestamp: {datetime.now().isoformat()}")
        
        if not self.authenticate():
            return
        
        # 1. Test produits - différents paramètres
        print("\n" + "="*70)
        print("🧪 TESTS PRODUITS")
        
        # Test basique
        self.debug_endpoint("products", {"page": 0, "size": 5}, "Test basique (5 premiers)")
        
        # Test avec plus d'éléments
        self.debug_endpoint("products", {"page": 0, "size": 50}, "Test étendu (50 premiers)")
        
        # Test sans pagination
        self.debug_endpoint("products", {}, "Sans pagination")
        
        # Test avec filtres
        self.debug_endpoint("products", {"active": True}, "Produits actifs seulement")
        
        # Test avec date
        yesterday = (datetime.now() - timedelta(days=1)).isoformat()
        self.debug_endpoint("products", {"from": yesterday}, "Modifiés depuis hier")
        
        # 2. Test commandes
        print("\n" + "="*70)
        print("🧪 TESTS COMMANDES")
        
        # Test basique
        self.debug_endpoint("orders", {"page": 0, "size": 10}, "Test basique")
        
        # Test sans filtre de date
        self.debug_endpoint("orders", {"page": 0, "size": 10}, "Sans filtre de date")
        
        # Test avec période plus large
        last_month = (datetime.now() - timedelta(days=30)).isoformat()
        self.debug_endpoint("orders", {"from": last_month, "page": 0, "size": 10}, "Dernier mois")
        
        # Test avec période encore plus large
        last_year = (datetime.now() - timedelta(days=365)).isoformat()
        self.debug_endpoint("orders", {"from": last_year, "page": 0, "size": 10}, "Dernière année")
        
        # 3. Test ventes
        print("\n" + "="*70)
        print("🧪 TESTS VENTES")
        
        # Test basique
        self.debug_endpoint("sales", {"page": 0, "size": 10}, "Test basique")
        
        # Test sans filtre de date
        self.debug_endpoint("sales", {"page": 0, "size": 10}, "Sans filtre de date")
        
        # Test avec période plus large
        self.debug_endpoint("sales", {"from": last_month, "page": 0, "size": 10}, "Dernier mois")
        
        # Test avec période encore plus large
        self.debug_endpoint("sales", {"from": last_year, "page": 0, "size": 10}, "Dernière année")
        
        print("\n" + "="*70)
        print("🎯 ANALYSE")
        print("Si tous les tests retournent 0 éléments:")
        print("1. 🏥 La pharmacie n'a peut-être pas de données")
        print("2. 🔐 L'accès est limité à certaines données")
        print("3. 📅 Les données sont dans une période différente")
        print("4. 🔧 L'API nécessite des paramètres spéciaux")
        print("\n💡 Solution: Contacter Cédric FONTERAY avec ces logs")


if __name__ == "__main__":
    debugger = ApothicalDebugger()
    debugger.run_full_debug()