#!/usr/bin/env python3
"""
Script de test pour l'endpoint ventes sur une période de deux jours
"""

import requests
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Variables d'environnement pour le test
SERVER_URL = os.environ.get('SERVER_URL', 'https://api.phardev.fr')
API_URL = os.environ.get('API_URL', 'YXBvdGhpY2Fs')
API_PASSWORD = os.environ.get('API_PASSWORD', 'cGFzczE')
IDNATS = os.environ.get('IDNATS', '832011373')

def test_ventes_periode(dt1, dt2, description):
    """
    Teste l'endpoint ventes avec une période spécifique
    
    Args:
        dt1 (str): Date de début au format YYYY-MM-DD
        dt2 (str): Date de fin au format YYYY-MM-DD
        description (str): Description de la période pour l'affichage
    """
    print(f"\n===== Test de l'endpoint ventes pour la période {description}: {dt1} à {dt2} =====")
    
    # Construire l'URL
    url = f"https://grpstat.winpharma.com/ApiWp/{API_URL}/ventes?password={API_PASSWORD}&Idnats={IDNATS}&dt1={dt1}&dt2={dt2}"
    print(f"URL: {url}")
    
    try:
        # Requête à l'API
        response = requests.get(url)
        
        # Traiter la réponse
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Succès! Statut: {response.status_code}")
            print(f"   Taille de la réponse: {len(json.dumps(data))} caractères")
            
            # Analyser les données
            if data and len(data) > 0:
                if 'ventes' in data[0]:
                    ventes = data[0]['ventes']
                    print(f"   Nombre de ventes: {len(ventes)}")
                    
                    # Analyser les dates
                    dates = {}
                    for vente in ventes:
                        date = vente['heure'].split('T')[0]
                        dates[date] = dates.get(date, 0) + 1
                    
                    print("   Répartition des ventes par jour:")
                    for date, count in sorted(dates.items()):
                        print(f"   - {date}: {count} ventes")
                    
                    # Enregistrer les données pour analyse
                    filename = f"ventes_{dt1}_to_{dt2}.json"
                    with open(filename, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    print(f"   Données enregistrées dans {filename}")
                    
                    # Essayer d'envoyer au serveur
                    print("\n   Envoi des données au serveur...")
                    server_response = requests.post(
                        f"{SERVER_URL}/winpharma_2/create/sales", 
                        json=data,
                        headers={'Pharmacy-id': IDNATS}
                    )
                    
                    if server_response.status_code == 200:
                        print(f"   ✅ Données envoyées avec succès! Statut: {server_response.status_code}")
                        print(f"      Réponse: {server_response.text}")
                        return True, ventes, dates
                    else:
                        print(f"   ❌ Erreur serveur: {server_response.status_code}")
                        print(f"      Réponse: {server_response.text}")
                else:
                    print(f"   ❌ Clé 'ventes' non trouvée dans les données")
            else:
                print(f"   ❌ Données vides ou mal structurées")
                
        elif response.status_code == 204:
            print(f"⚠️ Pas de contenu (204): Aucune vente trouvée pour cette période")
        elif response.status_code == 400 and "dt2 est postérieur aux données" in response.text:
            print(f"⚠️ Date future: {response.text}")
            
            # Essayer d'extraire la date maximale disponible
            import re
            match = re.search(r"dernière date disponible = (\d{4}-\d{2}-\d{2})", response.text)
            if match:
                max_date = match.group(1)
                print(f"   Date maximale disponible: {max_date}")
                
                if max_date <= dt1:
                    print(f"   ⚠️ La date maximale est antérieure à la date de début, impossible de continuer")
                    return False, None, None
                
                print(f"   Réessai avec la date maximale comme fin...")
                
                # Réessayer avec la date maximale
                url_corrected = url.replace(f"dt2={dt2}", f"dt2={max_date}")
                return test_ventes_periode(dt1, max_date, f"{description} (corrigée)")
        else:
            print(f"❌ Erreur: {response.status_code}")
            print(f"   {response.text}")
            
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
    
    return False, None, None

# Générer le code Lambda pour l'approche avec période spécifique
def generate_lambda_code(dt1, dt2):
    """
    Génère le code Lambda modifié pour récupérer les ventes avec une période spécifique
    
    Args:
        dt1 (str): Date de début
        dt2 (str): Date de fin
    """
    code = f"""# Modification à apporter au Lambda pour récupérer les ventes sur la période {dt1} à {dt2}

# Dans la fonction handler, remplacer la partie de calcul des dates par:
    # Dates spécifiques pour chaque endpoint
    for in_endpoint, out_endpoint in endpoints.items():
        print(f"Traitement de l'endpoint {{in_endpoint}}...")
        
        # Construire l'URL de base
        url = f"https://grpstat.winpharma.com/ApiWp/{{API_URL}}/{{in_endpoint}}?password={{API_PASSWORD}}&Idnats={{IDNATS}}"
        
        # Ajouter les paramètres de date selon l'endpoint
        if in_endpoint == 'ventes':
            # Pour les ventes: utiliser une période fixe de deux jours qui fonctionne
            url += f"&dt1={dt1}&dt2={dt2}"
        elif not full_dump:
            # Pour les autres endpoints: utiliser la plage habituelle (hier à aujourd'hui)
            dt2_current = datetime.now().strftime('%Y-%m-%d')
            dt1_current = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            url += f"&dt1={{dt1_current}}&dt2={{dt2_current}}"
"""
    return code

# Programme principal
if __name__ == "__main__":
    print("🔍 TEST DE L'ENDPOINT VENTES SUR DES PÉRIODES DE DEUX JOURS")
    print("----------------------------------------------------------")
    
    # Tester différentes périodes de deux jours
    # Modifier ces dates pour tester différentes périodes
    periodes = [
        # Juin 2025
        ("2025-06-01", "2025-06-02", "Début juin"),
        ("2025-06-10", "2025-06-11", "Mi-juin"),
        
        # Mai 2025
        ("2025-05-20", "2025-05-21", "Fin mai"),
        ("2025-05-10", "2025-05-11", "Mi-mai"),
        
        # Avril 2025
        ("2025-04-20", "2025-04-21", "Fin avril"),
        
        # Période plus longue pour comparaison
        ("2025-05-01", "2025-05-07", "Première semaine de mai")
    ]
    
    results = {}
    
    for dt1, dt2, desc in periodes:
        success, ventes, dates = test_ventes_periode(dt1, dt2, desc)
        if success:
            results[(dt1, dt2)] = {"description": desc, "ventes": len(ventes), "dates": dates}
    
    # Résumé
    print("\n===== RÉSUMÉ DES TESTS =====")
    if results:
        print("Période | Nombre de ventes | Répartition")
        print("------------------------------------------")
        for (dt1, dt2), result in sorted(results.items()):
            dates_str = ", ".join([f"{date}: {count}" for date, count in sorted(result["dates"].items())])
            print(f"{dt1} à {dt2} | {result['ventes']} | {dates_str}")
        
        # Trouver la meilleure période
        best_period = max(results.items(), key=lambda x: x[1]["ventes"])
        dt1, dt2 = best_period[0]
        
        print(f"\n🔍 RECOMMANDATION:")
        print(f"La période optimale pour récupérer les ventes est du {dt1} au {dt2}")
        print(f"Nombre de ventes: {best_period[1]['ventes']}")
        print(f"Répartition: {', '.join([f'{date}: {count}' for date, count in sorted(best_period[1]['dates'].items())])}")
        
        print("\nCode à modifier dans le Lambda:")
        print(generate_lambda_code(dt1, dt2))
    else:
        print("❌ Aucune période n'a fonctionné. Essayez d'autres dates.")