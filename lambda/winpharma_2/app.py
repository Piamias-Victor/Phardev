import json
import os
import re
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Variables d'environnement requises pour le Lambda
SERVER_URL = os.environ.get('SERVER_URL')
API_URL = os.environ.get('API_URL')     # YXBvdGhpY2Fs (Base64 pour "apothical")
API_PASSWORD = os.environ.get('API_PASSWORD')  # cGFzczE (Base64 pour "pass1")
IDNATS = os.environ.get('IDNATS', '062044623')


def handler(event, context, full_dump=True):
    """
    Fonction principale du Lambda qui récupère les données de l'API Winpharma 2
    et les envoie au serveur backend.
    
    Args:
        event: Événement déclencheur Lambda (non utilisé)
        context: Contexte Lambda (non utilisé)
        full_dump: Si True, récupère toutes les données, sinon les dernières 24h
        
    Returns:
        dict: Résultat de l'exécution avec les statuts pour chaque endpoint
    """
    # Vérifier que les variables d'environnement sont définies
    if not all([SERVER_URL, API_URL, API_PASSWORD]):
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": "Variables d'environnement manquantes. Vérifiez SERVER_URL, API_URL et API_PASSWORD."
            })
        }
    
    # Mapping des endpoints entre l'API et le serveur
    endpoints = {
        'produits': 'products',
        'achats': 'orders',   # Commenté temporairement en attendant la correction du serveur
        'ventes': 'sales'
    }
    
    # Résultats pour chaque endpoint
    results = {}
    
    # Dates pour les requêtes : J-2 à J-1
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    day_before_yesterday = today - timedelta(days=2)
    
    # Format des dates
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    day_before_yesterday_str = day_before_yesterday.strftime('%Y-%m-%d')
    
    # Traiter chaque endpoint
    for in_endpoint, out_endpoint in endpoints.items():
        print(f"Traitement de l'endpoint {in_endpoint}...")
        
        # Construire l'URL de base
        url = f"https://grpstat.winpharma.com/ApiWp/{API_URL}/{in_endpoint}?password={API_PASSWORD}&Idnats={IDNATS}"
        
        # Ajouter les paramètres de date : J-2 à J-1 pour tous les endpoints
        if not full_dump:
            # Par exemple, si on est le 18, on récupère du 16 au 17
            url += f"&dt1={day_before_yesterday_str}&dt2={yesterday_str}"
        
        try:
            print(f"Requête: {url}")
            response = requests.get(url)
            
            if response.status_code == 200:
                # Requête réussie
                data = response.json()
                print(f"Données récupérées: {len(json.dumps(data))} caractères")
                
                # Analyser la répartition des données (pour les ventes uniquement)
                if in_endpoint == 'ventes' and data and len(data) > 0 and 'ventes' in data[0]:
                    ventes = data[0]['ventes']
                    dates = {}
                    for vente in ventes:
                        date = vente['heure'].split('T')[0]
                        dates[date] = dates.get(date, 0) + 1
                    
                    print(f"Répartition des ventes par jour:")
                    for date, count in sorted(dates.items()):
                        print(f"- {date}: {count} ventes")
                
                # Envoyer les données au serveur
                server_response = requests.post(
                    f"{SERVER_URL}/winpharma_2/create/{out_endpoint}", 
                    json=data,
                    headers={'Pharmacy-id': IDNATS}
                )
                
                if server_response.status_code == 200:
                    print(f"Données envoyées avec succès au serveur pour {in_endpoint}")
                    results[in_endpoint] = "OK"
                else:
                    error_msg = f"Erreur serveur: {server_response.status_code}: {server_response.text}"
                    print(error_msg)
                    results[in_endpoint] = error_msg
            
            # Gérer l'erreur spécifique de date future
            elif response.status_code == 400 and "dt2 est postérieur aux données" in response.text:
                try:
                    # Extraire la date maximale disponible
                    match = re.search(r"dernière date disponible = (\d{4}-\d{2}-\d{2})", response.text)
                    if match:
                        max_date = match.group(1)
                        print(f"Date max disponible: {max_date}, réessai avec cette date")
                        
                        # Réessayer avec la date maximale
                        url_corrected = url.replace(f"dt2={yesterday_str}", f"dt2={max_date}")
                        print(f"Nouvelle requête: {url_corrected}")
                        
                        response = requests.get(url_corrected)
                        if response.status_code == 200:
                            data = response.json()
                            print(f"Données récupérées: {len(json.dumps(data))} caractères")
                            
                            # Analyser la répartition des données (pour les ventes uniquement)
                            if in_endpoint == 'ventes' and data and len(data) > 0 and 'ventes' in data[0]:
                                ventes = data[0]['ventes']
                                dates = {}
                                for vente in ventes:
                                    date = vente['heure'].split('T')[0]
                                    dates[date] = dates.get(date, 0) + 1
                                
                                print(f"Répartition des ventes par jour:")
                                for date, count in sorted(dates.items()):
                                    print(f"- {date}: {count} ventes")
                            
                            # Envoyer les données au serveur
                            server_response = requests.post(
                                f"{SERVER_URL}/winpharma_2/create/{out_endpoint}", 
                                json=data,
                                headers={'Pharmacy-id': IDNATS}
                            )
                            
                            if server_response.status_code == 200:
                                print(f"Données envoyées avec succès au serveur pour {in_endpoint}")
                                results[in_endpoint] = "OK"
                            else:
                                error_msg = f"Erreur serveur: {server_response.status_code}: {server_response.text}"
                                print(error_msg)
                                results[in_endpoint] = error_msg
                        elif response.status_code == 204:
                            # Pas de contenu pour cette date
                            print(f"Pas de données disponibles pour {in_endpoint} à la date {max_date}")
                            results[in_endpoint] = f"Pas de données disponibles pour la date {max_date}"
                        else:
                            error_msg = f"Erreur après correction de date: {response.status_code}: {response.text}"
                            print(error_msg)
                            results[in_endpoint] = error_msg
                    else:
                        error_msg = f"Impossible d'extraire la date max: {response.text}"
                        print(error_msg)
                        results[in_endpoint] = error_msg
                except Exception as e:
                    error_msg = f"Erreur lors du traitement de la correction de date: {str(e)}"
                    print(error_msg)
                    results[in_endpoint] = error_msg
            
            # Gérer le cas où il n'y a pas de données (204 No Content)
            elif response.status_code == 204:
                print(f"Pas de données disponibles pour {in_endpoint}")
                results[in_endpoint] = "Pas de données disponibles"
            
            # Autres erreurs
            else:
                error_msg = f"Erreur API: {response.status_code}: {response.text}"
                print(error_msg)
                results[in_endpoint] = error_msg
                
        except requests.exceptions.RequestException as e:
            error_msg = f"Erreur de connexion: {str(e)}"
            print(error_msg)
            results[in_endpoint] = error_msg
    
    # Retourner le résultat global
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Traitement terminé",
            "results": results
        })
    }


# Si exécuté directement (tests locaux)
if __name__ == "__main__":
    # Variables d'environnement pour les tests locaux
    if not os.environ.get('SERVER_URL'):
        os.environ['SERVER_URL'] = input("Entrez l'URL du serveur: ")
    if not os.environ.get('API_URL'):
        os.environ['API_URL'] = "YXBvdGhpY2Fs"  # Valeur par défaut
    if not os.environ.get('API_PASSWORD'):
        os.environ['API_PASSWORD'] = "cGFzczE"  # Valeur par défaut
    if not os.environ.get('IDNATS'):
        os.environ['IDNATS'] = "062044623"  # Valeur par défaut
    
    # Mode de test (full_dump = False pour récupérer seulement les dernières 24h)
    full_dump = input("Exécuter en mode full dump? (o/n): ").lower() == 'o'
    
    # Exécuter le handler
    result = handler(None, None, full_dump=full_dump)
    print("\nRésultat du Lambda:")
    print(json.dumps(result, indent=2))