import requests
import json
import os
from datetime import datetime, timedelta

# Paramètres de l'API
PHARMACY_ID = "YXBvdGhpY2Fs"  # Base64 pour "apothical"
PASSWORD = "cGFzczE"  # Base64 pour "pass1"
IDNATS = "832011373"

# Dates pour le test (utiliser une date fixe pour éviter les problèmes)
dt2 = "2025-06-17"  # Date maximale disponible selon l'erreur
dt1 = "2025-06-12"  # 5 jours avant

# Création du dossier pour stocker les résultats
if not os.path.exists("resultats_api"):
    os.makedirs("resultats_api")

# 1. Tester l'endpoint produits
print("\n------ Test de l'endpoint produits ------")
url = f"https://grpstat.winpharma.com/ApiWp/{PHARMACY_ID}/produits?password={PASSWORD}&Idnats={IDNATS}&dt1={dt1}&dt2={dt2}"
print(f"URL: {url}")
try:
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print(f"Statut: {response.status_code} OK")
        print(f"Taille de la réponse: {len(json.dumps(data))} caractères")
        
        with open("resultats_api/produits.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            print("Données sauvegardées dans resultats_api/produits.json")
        
        for block in data:
            if 'produits' in block and block['produits']:
                print(f"Nombre de produits: {len(block['produits'])}")
                print("Exemple de produit:")
                print(json.dumps(block['produits'][0], indent=2, ensure_ascii=False)[:500])
                break
    else:
        print(f"Erreur: {response.status_code}")
        print(response.text)
except Exception as e:
    print(f"Erreur de connexion: {e}")

# 2. Rechercher le bon endpoint pour les commandes
print("\n------ Recherche de l'endpoint commandes ------")
# Essayer différentes variantes pour trouver le bon endpoint
possible_endpoints = ["commandes", "achats", "orders", "achat"]
found = False

for endpoint in possible_endpoints:
    url = f"https://grpstat.winpharma.com/ApiWp/{PHARMACY_ID}/{endpoint}?password={PASSWORD}&Idnats={IDNATS}&dt1={dt1}&dt2={dt2}"
    print(f"Test de l'URL: {url}")
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Endpoint trouvé! Statut: {response.status_code} OK")
            print(f"Taille de la réponse: {len(json.dumps(data))} caractères")
            
            with open(f"resultats_api/{endpoint}.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                print(f"Données sauvegardées dans resultats_api/{endpoint}.json")
            
            # Analyser la structure pour voir où sont les commandes
            print("Structure de premier niveau:", list(data[0].keys()) if data else "Vide")
            found = True
            break
        else:
            print(f"Erreur: {response.status_code}")
    except Exception as e:
        print(f"Erreur: {e}")

if not found:
    print("❌ Aucun endpoint pour les commandes n'a été trouvé")

# 3. Tester l'endpoint ventes avec la date correcte
print("\n------ Test de l'endpoint ventes ------")
url = f"https://grpstat.winpharma.com/ApiWp/{PHARMACY_ID}/ventes?password={PASSWORD}&Idnats={IDNATS}&dt1={dt1}&dt2={dt2}"
print(f"URL: {url}")
try:
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print(f"Statut: {response.status_code} OK")
        print(f"Taille de la réponse: {len(json.dumps(data))} caractères")
        
        with open("resultats_api/ventes.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            print("Données sauvegardées dans resultats_api/ventes.json")
        
        for block in data:
            if 'ventes' in block and block['ventes']:
                print(f"Nombre de ventes: {len(block['ventes'])}")
                print("Exemple de vente:")
                print(json.dumps(block['ventes'][0], indent=2, ensure_ascii=False)[:500])
                break
    else:
        print(f"Erreur: {response.status_code}")
        print(response.text)
except Exception as e:
    print(f"Erreur de connexion: {e}")

print("\n✅ Test terminé! Les résultats sont dans le dossier 'resultats_api'")