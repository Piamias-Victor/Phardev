#!/usr/bin/env python3
"""
Test API Winpharma pour Pharmacie Becker Monteux (842004863)
Vérifie la récupération des produits, ventes et achats
"""

import requests
import json
from datetime import datetime, timedelta

# Configuration
API_URL = "YXBvdGhpY2Fs"
API_PASSWORD = "cGFzczE"
PHARMACY_ID = "842004863"  # Pharmacie Becker Monteux

def test_pharmacy_api():
    """Test complet pour une pharmacie"""
    
    print(f"🔍 TEST API - Pharmacie Becker Monteux ({PHARMACY_ID})")
    print("="*60)
    
    # 1. TEST PRODUITS
    print("\n📦 TEST 1: PRODUITS")
    print("-"*40)
    
    url = f"https://grpstat.winpharma.com/ApiWp/{API_URL}/produits"
    params = {
        'password': API_PASSWORD,
        'Idnats': PHARMACY_ID
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        print(f"Status code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            if data and isinstance(data, list) and len(data) > 0:
                products = data[0].get('produits', [])
                print(f"✅ {len(products)} produits trouvés")
                
                # Afficher 3 exemples
                for i, prod in enumerate(products[:3], 1):
                    print(f"\nProduit {i}:")
                    print(f"  ID: {prod.get('ProdId')}")
                    print(f"  Nom: {prod.get('Nom')}")
                    print(f"  Code13Ref: {prod.get('Code13Ref')}")
                    print(f"  Stock: {prod.get('Stock')}")
                    print(f"  PrixTTC: {prod.get('PrixTTC')}")
            else:
                print("❌ Aucun produit ou structure inattendue")
                
        elif response.status_code == 401:
            print("❌ Erreur authentification - Credentials invalides")
            return
        else:
            print(f"❌ Erreur: {response.status_code}")
            print(f"Message: {response.text}")
            
    except Exception as e:
        print(f"❌ Exception: {e}")
    
    # 2. TEST VENTES (derniers 7 jours)
    print("\n📊 TEST 2: VENTES")
    print("-"*40)
    
    date_end = datetime.now()
    date_start = date_end - timedelta(days=7)
    
    url = f"https://grpstat.winpharma.com/ApiWp/{API_URL}/ventes"
    params = {
        'password': API_PASSWORD,
        'Idnats': PHARMACY_ID,
        'dt1': date_start.strftime('%Y-%m-%d'),
        'dt2': date_end.strftime('%Y-%m-%d')
    }
    
    print(f"Période: {params['dt1']} → {params['dt2']}")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        print(f"Status code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            if data and isinstance(data, list) and len(data) > 0:
                ventes = data[0].get('ventes', [])
                print(f"✅ {len(ventes)} ventes trouvées")
                
                # Analyser les TVA
                tva_count = 0
                tva_values = set()
                
                for vente in ventes[:10]:  # Analyser les 10 premières
                    for ligne in vente.get('lignes', []):
                        if 'tva' in ligne and ligne['tva'] is not None:
                            tva_count += 1
                            tva_values.add(ligne['tva'])
                
                print(f"\nAnalyse TVA sur les 10 premières ventes:")
                print(f"  Lignes avec TVA: {tva_count}")
                print(f"  Valeurs TVA trouvées: {sorted(tva_values)}")
                
                # Exemple de vente
                if ventes:
                    vente = ventes[0]
                    print(f"\nExemple vente:")
                    print(f"  ID: {vente.get('id')}")
                    print(f"  Heure: {vente.get('heure')}")
                    print(f"  Nb lignes: {len(vente.get('lignes', []))}")
                    
                    if vente.get('lignes'):
                        ligne = vente['lignes'][0]
                        print(f"  Première ligne:")
                        print(f"    ProdId: {ligne.get('prodId')}")
                        print(f"    Qte: {ligne.get('qte')}")
                        print(f"    Prix: {ligne.get('prix')}")
                        print(f"    TVA: {ligne.get('tva')}")
            else:
                print("⚠️ Aucune vente sur cette période")
                
        elif response.status_code == 204:
            print("ℹ️ Pas de données sur cette période")
        else:
            print(f"❌ Erreur: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Exception: {e}")
    
    # 3. TEST ACHATS (derniers 30 jours)
    print("\n📦 TEST 3: ACHATS/COMMANDES")
    print("-"*40)
    
    date_start = date_end - timedelta(days=30)
    
    url = f"https://grpstat.winpharma.com/ApiWp/{API_URL}/achats"
    params = {
        'password': API_PASSWORD,
        'Idnats': PHARMACY_ID,
        'dt1': date_start.strftime('%Y-%m-%d'),
        'dt2': date_end.strftime('%Y-%m-%d')
    }
    
    print(f"Période: {params['dt1']} → {params['dt2']}")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        print(f"Status code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            if data and isinstance(data, list) and len(data) > 0:
                achats = data[0].get('achats', [])
                print(f"✅ {len(achats)} commandes trouvées")
                
                if achats:
                    achat = achats[0]
                    print(f"\nExemple commande:")
                    print(f"  ID: {achat.get('id')}")
                    print(f"  Fournisseur: {achat.get('nomFourn')}")
                    print(f"  Date envoi: {achat.get('dateEnvoi')}")
                    print(f"  Date livraison: {achat.get('dateLivraison')}")
                    print(f"  Channel: {achat.get('channel')}")
            else:
                print("⚠️ Aucune commande sur cette période")
                
        else:
            print(f"❌ Erreur: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Exception: {e}")
    
    print("\n" + "="*60)
    print("✅ Test terminé")

if __name__ == "__main__":
    test_pharmacy_api()