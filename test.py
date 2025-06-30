#!/usr/bin/env python3
"""
Script de test API WinPharma pour le 24 juin 2025
"""

import requests
import json
from datetime import datetime, timedelta

# Configuration
API_URL = "YXBvdGhpY2Fs"
API_PASSWORD = "cGFzczE" 
PHARMACY_ID = "832011373"
BASE_URL = "https://grpstat.winpharma.com/ApiWp"

# Date à tester
TEST_DATE = "2025-06-24"

print(f"🔍 TEST API WINPHARMA")
print(f"📅 Date ciblée: {TEST_DATE}")
print(f"🏥 Pharmacie: {PHARMACY_ID}")
print(f"=" * 60)

def test_api_endpoint(endpoint, date_start, date_end=None):
    """
    Teste un endpoint spécifique de l'API
    
    Args:
        endpoint: 'ventes', 'achats', ou 'produits'
        date_start: Date de début (YYYY-MM-DD)
        date_end: Date de fin (YYYY-MM-DD) - optionnel pour produits
        
    IMPORTANT: Pour récupérer les ventes du jour X, utiliser dt1=X-1, dt2=X
    """
    print(f"\n📡 TESTING ENDPOINT: {endpoint}")
    print(f"🔗 Date range: {date_start}" + (f" → {date_end}" if date_end else " (single date)"))
    print("-" * 40)
    
    # Construire l'URL
    url = f"{BASE_URL}/{API_URL}/{endpoint}"
    
    # Paramètres de base
    params = {
        'password': API_PASSWORD,
        'Idnats': PHARMACY_ID
    }
    
    # Ajouter dates pour ventes et achats
    if endpoint in ['ventes', 'achats']:
        params['dt1'] = date_start
        params['dt2'] = date_end or date_start
    
    try:
        print(f"🔍 URL: {url}")
        print(f"📝 Params: {json.dumps(params, indent=2)}")
        
        # Appel API
        response = requests.get(url, params=params, timeout=30)
        
        print(f"📊 Status Code: {response.status_code}")
        print(f"📏 Response Length: {len(response.text)} chars")
        
        if response.status_code == 200:
            try:
                data = response.json()
                print(f"✅ JSON VALID")
                
                # Analyser la structure
                if isinstance(data, list) and len(data) > 0:
                    pharmacy_data = data[0]
                    print(f"🏥 Pharmacy ID in response: {pharmacy_data.get('cip_pharma', 'N/A')}")
                    
                    if endpoint in pharmacy_data:
                        records = pharmacy_data[endpoint]
                        print(f"📊 {endpoint.upper()} COUNT: {len(records)}")
                        
                        # Afficher quelques exemples
                        if len(records) > 0:
                            print(f"\n📋 STRUCTURE (premier élément):")
                            first_record = records[0]
                            for key, value in first_record.items():
                                if isinstance(value, list):
                                    print(f"   {key}: [liste de {len(value)} éléments]")
                                elif isinstance(value, dict):
                                    print(f"   {key}: {{dictionnaire}}")
                                else:
                                    print(f"   {key}: {value}")
                                    
                            # Pour les ventes, afficher détail des lignes
                            if endpoint == 'ventes' and 'lignes' in first_record:
                                lignes = first_record['lignes']
                                print(f"\n🛒 PREMIÈRE VENTE - LIGNES ({len(lignes)} produits):")
                                total_qty = 0
                                for i, ligne in enumerate(lignes[:3]):  # 3 premiers produits
                                    qty = ligne.get('qte', 0)
                                    total_qty += qty
                                    print(f"   Ligne {i+1}: {ligne.get('nomProduit', 'N/A')} (qty: {qty})")
                                if len(lignes) > 3:
                                    print(f"   ... et {len(lignes) - 3} autres produits")
                                print(f"   📦 Total quantité vente: {total_qty}")
                                
                            # Statistiques par date si applicable
                            if endpoint in ['ventes', 'achats']:
                                dates_count = {}
                                total_lines = 0
                                
                                for record in records:
                                    if endpoint == 'ventes':
                                        date_field = 'heure'
                                        lines_field = 'lignes'
                                    else:  # achats
                                        date_field = 'dateLivraison'
                                        lines_field = 'lignes'
                                    
                                    record_date = record.get(date_field, '')[:10]
                                    if record_date:
                                        dates_count[record_date] = dates_count.get(record_date, 0) + 1
                                        
                                        if lines_field in record:
                                            total_lines += len(record[lines_field])
                                
                                print(f"\n📅 RÉPARTITION PAR DATE:")
                                for date, count in sorted(dates_count.items()):
                                    print(f"   {date}: {count} {endpoint}")
                                    
                                print(f"📈 TOTAL LIGNES: {total_lines}")
                        else:
                            print(f"⚠️ {endpoint.upper()}: Aucun enregistrement trouvé")
                    else:
                        print(f"❌ Clé '{endpoint}' non trouvée dans la réponse")
                        print(f"🔑 Clés disponibles: {list(pharmacy_data.keys())}")
                else:
                    print(f"❌ Structure de réponse inattendue")
                    
            except json.JSONDecodeError as e:
                print(f"❌ JSON INVALID: {e}")
                print(f"📄 Raw response (first 500 chars): {response.text[:500]}")
                
        elif response.status_code == 204:
            print(f"ℹ️ NO DATA: Aucune donnée pour cette période")
            
        elif response.status_code == 400:
            print(f"❌ BAD REQUEST: {response.text}")
            
        elif response.status_code == 401:
            print(f"❌ UNAUTHORIZED: Vérifiez vos identifiants")
            
        elif response.status_code == 404:
            print(f"❌ NOT FOUND: Endpoint ou pharmacie introuvable")
            
        else:
            print(f"❌ ERROR {response.status_code}: {response.text}")
            
        return {
            'status_code': response.status_code,
            'success': response.status_code == 200,
            'data_available': response.status_code == 200
        }
        
    except requests.exceptions.Timeout:
        print(f"⏱️ TIMEOUT: La requête a pris trop de temps")
        return {'status_code': 'timeout', 'success': False}
        
    except requests.exceptions.ConnectionError as e:
        print(f"🔌 CONNECTION ERROR: {e}")
        return {'status_code': 'connection_error', 'success': False}
        
    except Exception as e:
        print(f"💥 UNEXPECTED ERROR: {e}")
        return {'status_code': 'error', 'success': False}

def main():
    """
    Test principal pour le 24 juin 2025
    CORRECTION: Pour avoir les ventes du 24, utiliser période 23→24
    """
    endpoints_to_test = ['ventes', 'achats', 'produits']
    results = {}
    
    # Date ciblée : 24 juin
    target_date = TEST_DATE
    # Pour avoir les ventes du 24, période : 23→24
    date_start = "2025-06-23"
    date_end = "2025-06-24"
    
    print(f"🎯 CORRECTION: Pour récupérer les ventes du {target_date}")
    print(f"📅 Utilisation de la période: {date_start} → {date_end}")
    print()
    
    for endpoint in endpoints_to_test:
        if endpoint == 'produits':
            # Produits : pas de paramètre de date
            result = test_api_endpoint(endpoint, None)
        else:
            # Ventes et achats : période corrigée
            result = test_api_endpoint(endpoint, date_start, date_end)
            
        results[endpoint] = result
    
    # Résumé final
    print(f"\n" + "=" * 60)
    print(f"🏁 RÉSUMÉ FINAL - TEST DU {TEST_DATE}")
    print(f"=" * 60)
    
    for endpoint, result in results.items():
        status = "✅" if result['success'] else "❌"
        print(f"{status} {endpoint.upper()}: {result['status_code']}")
    
    success_count = sum(1 for r in results.values() if r['success'])
    total_count = len(results)
    
    if success_count == total_count:
        print(f"\n🎉 EXCELLENT! Tous les endpoints fonctionnent ({success_count}/{total_count})")
    elif success_count > 0:
        print(f"\n⚠️ PARTIEL: {success_count}/{total_count} endpoints OK")
    else:
        print(f"\n💥 PROBLÈME: Aucun endpoint ne fonctionne")
        
    # Test supplémentaire : vérifier si c'est un problème de date
    if not results.get('ventes', {}).get('success'):
        print(f"\n🔍 TEST SUPPLÉMENTAIRE: Vérification avec hier...")
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        test_api_endpoint('ventes', yesterday, yesterday)

if __name__ == "__main__":
    main()