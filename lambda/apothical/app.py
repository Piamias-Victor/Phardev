import os
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Configuration
SERVER_URL = os.environ.get('SERVER_URL')
FINESS_CODE = "712006733"  # Pharmacie Puig Léveilé (la seule accessible pour le moment)


def handler(event, context):
    """
    Lambda function pour récupérer automatiquement les données Apothical
    """
    
    if not SERVER_URL:
        print("❌ SERVER_URL non configuré")
        return {
            'statusCode': 500,
            'body': 'SERVER_URL manquant'
        }
    
    # Headers avec le code FINESS
    headers = {
        'Pharmacy-Finess': FINESS_CODE,
        'Content-Type': 'application/json'
    }
    
    # Endpoints à traiter
    endpoints = {
        'products': 'produits',
        'orders': 'commandes',
        'sales': 'ventes'
    }
    
    results = {}
    
    for endpoint, description in endpoints.items():
        url = f"{SERVER_URL}/apothical/create/{endpoint}"
        
        try:
            print(f"🔄 Traitement {description}...")
            
            # Appel POST sans body (les données sont récupérées via l'API dans le service)
            response = requests.post(url, json={}, headers=headers, timeout=300)
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ {description}: {data.get('message', 'OK')}")
                
                # Log des statistiques si disponibles
                if 'products_count' in data:
                    print(f"   📦 {data['products_count']} produits traités")
                if 'snapshots_count' in data:
                    print(f"   📊 {data['snapshots_count']} snapshots créés")
                if 'orders_count' in data:
                    print(f"   📋 {data['orders_count']} commandes traitées")
                if 'suppliers_count' in data:
                    print(f"   🏢 {data['suppliers_count']} fournisseurs traités")
                
                results[endpoint] = True
                
            else:
                print(f"❌ {description}: {response.status_code}")
                try:
                    error_data = response.json()
                    print(f"   Erreur: {error_data.get('message', 'Erreur inconnue')}")
                except:
                    print(f"   Réponse brute: {response.text}")
                
                results[endpoint] = False
                
        except requests.exceptions.Timeout:
            print(f"⏱️ {description}: Timeout (>5min)")
            results[endpoint] = False
            
        except requests.exceptions.RequestException as e:
            print(f"❌ {description}: Erreur connexion - {e}")
            results[endpoint] = False
            
        except Exception as e:
            print(f"❌ {description}: Erreur inattendue - {e}")
            results[endpoint] = False
    
    # Résumé final
    success_count = sum(results.values())
    total_count = len(results)
    
    print(f"\n📈 Résumé: {success_count}/{total_count} endpoints traités avec succès")
    
    if success_count == total_count:
        print("✅ Synchronisation Apothical terminée avec succès")
        status_code = 200
        message = "Synchronisation complète réussie"
    elif success_count > 0:
        print("⚠️ Synchronisation Apothical partiellement réussie")
        status_code = 206
        message = f"Synchronisation partielle: {success_count}/{total_count}"
    else:
        print("❌ Échec complet de la synchronisation Apothical")
        status_code = 500
        message = "Échec complet de la synchronisation"
    
    return {
        'statusCode': status_code,
        'body': {
            'message': message,
            'finess': FINESS_CODE,
            'timestamp': datetime.now().isoformat(),
            'results': results
        }
    }


# Pour test local
if __name__ == "__main__":
    print("🧪 Test local de la Lambda Apothical")
    print("=" * 50)
    
    result = handler({}, {})
    
    print(f"\nRésultat: {result}")