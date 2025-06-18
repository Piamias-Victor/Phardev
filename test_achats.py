#!/usr/bin/env python3
"""
Script de test pour l'endpoint achats
Ce script récupère et analyse les données de l'endpoint achats pour identifier le problème
"""

import requests
import json
import os
from datetime import datetime, timedelta

# Variables pour l'API
API_URL = "YXBvdGhpY2Fs"  # Base64 pour "apothical"
API_PASSWORD = "cGFzczE"  # Base64 pour "pass1"
IDNATS = "832011373"

# Calculer les dates: J-2 à J-1
today = datetime.now()
yesterday = today - timedelta(days=1)
day_before_yesterday = today - timedelta(days=2)

yesterday_str = yesterday.strftime('%Y-%m-%d')
day_before_yesterday_str = day_before_yesterday.strftime('%Y-%m-%d')

# Construire l'URL
url = f"https://grpstat.winpharma.com/ApiWp/{API_URL}/achats?password={API_PASSWORD}&Idnats={IDNATS}&dt1={day_before_yesterday_str}&dt2={yesterday_str}"
print(f"Requête: {url}")

try:
    # Requête à l'API
    response = requests.get(url)
    
    # Traiter la réponse
    if response.status_code == 200:
        data = response.json()
        print(f"✅ Succès! Statut: {response.status_code}")
        print(f"Taille de la réponse: {len(json.dumps(data))} caractères")
        
        # Enregistrer les données brutes
        with open("achats_data.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Données complètes enregistrées dans achats_data.json")
        
        # Analyser la structure
        if isinstance(data, list) and len(data) > 0:
            print(f"\nStructure de premier niveau: {list(data[0].keys())}")
            
            if 'achats' in data[0]:
                achats = data[0]['achats']
                print(f"Nombre d'achats: {len(achats)}")
                
                if achats:
                    achat = achats[0]
                    print("\nExemple d'achat:")
                    for key, value in achat.items():
                        print(f"- {key}: {value}")
                    
                    if 'lignes' in achat:
                        lignes = achat['lignes']
                        print(f"\nNombre de lignes: {len(lignes)}")
                        
                        if lignes:
                            ligne = lignes[0]
                            print("\nExemple de ligne:")
                            for key, value in ligne.items():
                                print(f"- {key}: {value} (type: {type(value).__name__})")
                    else:
                        print("❌ Clé 'lignes' non trouvée dans l'achat")
            else:
                print("❌ Clé 'achats' non trouvée")
        
        # Créer un échantillon minimal
        sample_data = [{
            "cip_pharma": data[0]["cip_pharma"],
            "achats": [data[0]['achats'][0]]
        }]
        
        with open("achats_sample.json", "w", encoding="utf-8") as f:
            json.dump(sample_data, f, ensure_ascii=False, indent=2)
        print("\nÉchantillon minimal créé dans achats_sample.json")
        
    elif response.status_code == 400 and "dt2 est postérieur aux données" in response.text:
        print(f"⚠️ Date future: {response.text}")
    else:
        print(f"❌ Erreur: {response.status_code}")
        print(f"   {response.text}")
except Exception as e:
    print(f"❌ Exception: {e}")