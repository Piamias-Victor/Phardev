# data/views2.py
"""
Nouveau fichier views pour repartir à zéro
Contient uniquement l'endpoint de création de pharmacie et récupération des produits
"""

import uuid
import logging
import requests
from django.db import transaction
from rest_framework.decorators import api_view
from rest_framework.response import Response

from data.services import winpharma_historical
from data.models import Pharmacy
from data.services import winpharma_new_api, winpharma_2  # ← Ajouter winpharma_2

logger = logging.getLogger(__name__)

# Configuration API WinPharma
BASE_URL = "https://grpstat.winpharma.com/ApiWp"
API_URL = "YXBvdGhpY2Fs"
API_PASSWORD = "cGFzczE"

@api_view(['POST'])
def create_pharmacy_v2(request):
    """
    Endpoint pour créer une nouvelle pharmacie - Version 2
    POST /api/v2/pharmacy/create
    Body: {"name": "Pharmacie Mouysset V2", "id_nat": "832002810"}
    """
    try:
        from django.db import connection
        print(f"=== DATABASE CONFIG ===")
        print(f"Database: {connection.settings_dict}")
        print(f"======================")
        
        data = request.data
        pharmacy_name = data.get('name')
        id_nat = data.get('id_nat')
        
        if not pharmacy_name:
            return Response({
                'error': 'Nom de pharmacie requis'
            }, status=400)
        
        if not id_nat:
            return Response({
                'error': 'ID national requis'
            }, status=400)
        
        # Vérifier si la pharmacie existe déjà (par nom OU par id_nat)
        existing_pharmacy = None
        if Pharmacy.objects.filter(name=pharmacy_name).exists():
            existing_pharmacy = Pharmacy.objects.get(name=pharmacy_name)
            logger.info(f"Pharmacie trouvée par nom: {pharmacy_name}")
        elif Pharmacy.objects.filter(id_nat=id_nat).exists():
            existing_pharmacy = Pharmacy.objects.get(id_nat=id_nat)
            logger.info(f"Pharmacie trouvée par id_nat: {id_nat}")
            
        if existing_pharmacy:
            return Response({
                'message': f"Pharmacie '{pharmacy_name}' existe déjà",
                'pharmacy_id': str(existing_pharmacy.id),
                'id_nat': existing_pharmacy.id_nat,
                'status': 'exists'
            }, status=200)
        
        # Créer la nouvelle pharmacie
        with transaction.atomic():
            pharmacy = Pharmacy.objects.create(
                id=uuid.uuid4(),
                name=pharmacy_name,
                id_nat=id_nat,
                address="Adresse import",
                area="Région import",
                ca=0,
                employees_count=1
            )
            
        logger.info(f"Nouvelle pharmacie créée: {pharmacy_name} ({id_nat})")
        
        return Response({
            'message': f"Pharmacie '{pharmacy_name}' créée avec succès",
            'pharmacy_id': str(pharmacy.id),
            'id_nat': pharmacy.id_nat,
            'status': 'created'
        }, status=201)
        
    except Exception as e:
        logger.error(f"Erreur création pharmacie: {e}")
        return Response({
            'error': 'Erreur serveur interne'
        }, status=500)


def fetch_winpharma_products(id_nat: str) -> dict:
    """
    Récupère les produits depuis l'API WinPharma pour une pharmacie
    """
    url = f"{BASE_URL}/{API_URL}/produits"
    params = {
        'password': API_PASSWORD,
        'Idnats': id_nat
    }
    
    try:
        # TIMEOUT ÉTENDU de 30s à 180s (3 minutes)
        response = requests.get(url, params=params, timeout=180)
        if response.status_code == 200:
            data = response.json()
            logger.info(f"Produits récupérés pour {id_nat}: {len(str(data))} caractères")
            return data
        elif response.status_code == 204:
            logger.info(f"Pas de produits pour {id_nat}")
            return []
        else:
            logger.error(f"Erreur API {response.status_code}: {response.text}")
            return None
    except Exception as e:
        logger.error(f"Erreur requête produits pour {id_nat}: {e}")
        return None


@api_view(['POST'])
def fetch_and_process_products_v2(request):
    """
    Endpoint pour récupérer et traiter les produits d'une pharmacie - Version 2
    POST /api/v2/pharmacy/products/fetch
    Body: {"id_nat": "832002810"} OU {"pharmacy_name": "Pharmacie Mouysset V2"}
    """
    try:
        data = request.data
        id_nat = data.get('id_nat')
        pharmacy_name = data.get('pharmacy_name')
        
        # Récupérer la pharmacie
        pharmacy = None
        if id_nat:
            try:
                pharmacy = Pharmacy.objects.get(id_nat=id_nat)
            except Pharmacy.DoesNotExist:
                return Response({
                    'error': f'Pharmacie avec id_nat {id_nat} non trouvée'
                }, status=404)
        elif pharmacy_name:
            try:
                pharmacy = Pharmacy.objects.get(name=pharmacy_name)
            except Pharmacy.DoesNotExist:
                return Response({
                    'error': f'Pharmacie avec nom {pharmacy_name} non trouvée'
                }, status=404)
        else:
            return Response({
                'error': 'id_nat ou pharmacy_name requis'
            }, status=400)
        
        logger.info(f"Récupération des produits pour {pharmacy.name} ({pharmacy.id_nat})")
        
        # Récupérer les données de l'API WinPharma
        products_data = fetch_winpharma_products(pharmacy.id_nat)
        
        if products_data is None:
            return Response({
                'error': 'Erreur lors de la récupération des produits depuis l\'API'
            }, status=500)
        
        if not products_data:
            return Response({
                'message': 'Aucun produit trouvé pour cette pharmacie',
                'pharmacy_id': str(pharmacy.id),
                'pharmacy_name': pharmacy.name,
                'products_count': 0
            }, status=200)
        
        # Traiter les produits avec le service existant
        try:
            result = winpharma_new_api.process_product(pharmacy, products_data)
            
            return Response({
                'message': f"Produits traités avec succès pour {pharmacy.name}",
                'pharmacy_id': str(pharmacy.id),
                'pharmacy_name': pharmacy.name,
                'id_nat': pharmacy.id_nat,
                'stats': {
                    'products_processed': len(result.get('products', [])),
                    'snapshots_created': len(result.get('snapshots', [])),
                    'raw_data_size': len(str(products_data))
                },
                'status': 'success'
            }, status=200)
            
        except Exception as e:
            logger.error(f"Erreur traitement produits: {e}")
            return Response({
                'error': f'Erreur lors du traitement des produits: {str(e)}',
                'pharmacy_id': str(pharmacy.id),
                'pharmacy_name': pharmacy.name
            }, status=500)
        
    except Exception as e:
        logger.error(f"Erreur générale fetch_and_process_products_v2: {e}")
        return Response({
            'error': 'Erreur serveur interne'
        }, status=500)


@api_view(['POST'])
def create_pharmacy_and_fetch_products_v2(request):
    """
    Endpoint combiné pour créer une pharmacie ET récupérer ses produits - Version 2
    POST /api/v2/pharmacy/create-and-fetch
    Body: {"name": "Pharmacie Mouysset V2", "id_nat": "832002810"}
    """
    try:
        data = request.data
        pharmacy_name = data.get('name')
        id_nat = data.get('id_nat')
        
        if not pharmacy_name or not id_nat:
            return Response({
                'error': 'name et id_nat requis'
            }, status=400)
        
        # 1. Créer ou récupérer la pharmacie (logique directe)
        logger.info(f"Création/récupération pharmacie: {pharmacy_name} ({id_nat})")
        
        # Vérifier si la pharmacie existe déjà
        existing_pharmacy = None
        if Pharmacy.objects.filter(name=pharmacy_name).exists():
            existing_pharmacy = Pharmacy.objects.get(name=pharmacy_name)
            logger.info(f"Pharmacie trouvée par nom: {pharmacy_name}")
        elif Pharmacy.objects.filter(id_nat=id_nat).exists():
            existing_pharmacy = Pharmacy.objects.get(id_nat=id_nat)
            logger.info(f"Pharmacie trouvée par id_nat: {id_nat}")
        
        if existing_pharmacy:
            pharmacy = existing_pharmacy
            pharmacy_status = 'exists'
            pharmacy_message = f"Pharmacie '{pharmacy_name}' existe déjà"
        else:
            # Créer la nouvelle pharmacie
            with transaction.atomic():
                pharmacy = Pharmacy.objects.create(
                    id=uuid.uuid4(),
                    name=pharmacy_name,
                    id_nat=id_nat,
                    address="Adresse import",
                    area="Région import",
                    ca=0,
                    employees_count=1
                )
            pharmacy_status = 'created'
            pharmacy_message = f"Pharmacie '{pharmacy_name}' créée avec succès"
            logger.info(f"Nouvelle pharmacie créée: {pharmacy_name} ({id_nat})")
        
        # 2. Récupérer et traiter les produits
        logger.info(f"Récupération des produits pour {pharmacy.name}")
        
        products_data = fetch_winpharma_products(pharmacy.id_nat)
        
        if products_data is None:
            products_result = {
                'message': 'Erreur lors de la récupération des produits depuis l\'API',
                'status': 'error',
                'stats': {},
                'error': 'API WinPharma indisponible'
            }
        elif not products_data:
            products_result = {
                'message': 'Aucun produit trouvé pour cette pharmacie',
                'status': 'success',
                'stats': {'products_processed': 0, 'snapshots_created': 0},
                'error': None
            }
        else:
            # Traiter les produits
            try:
                result = winpharma_new_api.process_product(pharmacy, products_data)
                products_result = {
                    'message': f"Produits traités avec succès pour {pharmacy.name}",
                    'status': 'success',
                    'stats': {
                        'products_processed': len(result.get('products', [])),
                        'snapshots_created': len(result.get('snapshots', [])),
                        'raw_data_size': len(str(products_data))
                    },
                    'error': None
                }
            except Exception as e:
                logger.error(f"Erreur traitement produits: {e}")
                products_result = {
                    'message': 'Erreur lors du traitement des produits',
                    'status': 'error',
                    'stats': {},
                    'error': str(e)
                }
        
        # 3. Construire la réponse combinée
        combined_response = {
            'pharmacy': {
                'message': pharmacy_message,
                'pharmacy_id': str(pharmacy.id),
                'id_nat': pharmacy.id_nat,
                'status': pharmacy_status
            },
            'products': products_result,
            'overall_status': 'success' if products_result['status'] == 'success' else 'partial_success'
        }
        
        return Response(combined_response, status=200)
        
    except Exception as e:
        logger.error(f"Erreur create_pharmacy_and_fetch_products_v2: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return Response({
            'error': 'Erreur serveur interne lors du traitement combiné',
            'details': str(e)
        }, status=500)

@api_view(['POST'])
def fetch_and_process_sales_v2(request):
    """
    Endpoint pour récupérer et traiter les ventes d'une pharmacie - Version 2
    POST /api/v2/pharmacy/sales/fetch
    Body: {"id_nat": "832002810"}
    PÉRIODE FIXE: avril à août 2025
    """
    try:
        data = request.data
        id_nat = data.get('id_nat')
        pharmacy_name = data.get('pharmacy_name')
        
        # Récupérer la pharmacie
        pharmacy = None
        if id_nat:
            try:
                pharmacy = Pharmacy.objects.get(id_nat=id_nat)
            except Pharmacy.DoesNotExist:
                return Response({
                    'error': f'Pharmacie avec id_nat {id_nat} non trouvée'
                }, status=404)
        elif pharmacy_name:
            try:
                pharmacy = Pharmacy.objects.get(name=pharmacy_name)
            except Pharmacy.DoesNotExist:
                return Response({
                    'error': f'Pharmacie avec nom {pharmacy_name} non trouvée'
                }, status=404)
        else:
            return Response({
                'error': 'id_nat ou pharmacy_name requis'
            }, status=400)
        
        logger.info(f"Récupération des ventes pour {pharmacy.name} ({pharmacy.id_nat}) - Période FIXE: avril-août 2025")
        
        # Récupérer les données de ventes de l'API WinPharma (période fixe)
        sales_data = fetch_winpharma_sales(pharmacy.id_nat)
        
        if sales_data is None:
            return Response({
                'error': 'Erreur lors de la récupération des ventes depuis l\'API'
            }, status=500)
        
        if not sales_data:
            return Response({
                'message': 'Aucune vente trouvée pour la période avril-août 2025',
                'pharmacy_id': str(pharmacy.id),
                'pharmacy_name': pharmacy.name,
                'period': "2025-04-01 -> 2025-08-31 (période fixe)",
                'sales_count': 0
            }, status=200)
        
        # Traiter les ventes avec le service
        try:
            winpharma_2.process_sales(pharmacy, sales_data)
            
            return Response({
                'message': f"Ventes traitées avec succès pour {pharmacy.name}",
                'pharmacy_id': str(pharmacy.id),
                'pharmacy_name': pharmacy.name,
                'id_nat': pharmacy.id_nat,
                'period': "2025-04-01 -> 2025-08-31 (période fixe)",
                'stats': {
                    'raw_data_size': len(str(sales_data)),
                    'data_blocks': len(sales_data),
                    'period_note': 'Données récupérées mois par mois (avril-août 2025)'
                },
                'status': 'success'
            }, status=200)
            
        except Exception as e:
            logger.error(f"Erreur traitement ventes: {e}")
            return Response({
                'error': f'Erreur lors du traitement des ventes: {str(e)}',
                'pharmacy_id': str(pharmacy.id),
                'pharmacy_name': pharmacy.name
            }, status=500)
        
    except Exception as e:
        logger.error(f"Erreur générale fetch_and_process_sales_v2: {e}")
        return Response({
            'error': 'Erreur serveur interne'
        }, status=500)


def fetch_winpharma_sales(id_nat: str) -> dict:
    """
    Récupère les ventes depuis l'API WinPharma pour une pharmacie
    FAIT DES APPELS MOIS PAR MOIS (avril à août 2025)
    VERSION CORRIGÉE INSPIRÉE DU SCRIPT POUR ACHATS
    """
    
    # Mois fixes à traiter 
    months = [
        ("2025-04-01", "2025-04-30"),
        ("2025-05-01", "2025-05-31"), 
        ("2025-06-01", "2025-06-30"),
        ("2025-07-01", "2025-07-31"),
        ("2025-08-01", "2025-08-31")
    ]
    
    all_data = []
    
    print(f"[SALES DEBUG] Début récupération ventes pour {id_nat}")
    print(f"[SALES DEBUG] Traitement de {len(months)} mois fixes")
    
    for i, (month_start, month_end) in enumerate(months, 1):
        print(f"[SALES DEBUG] === MOIS {i}/5: {month_start} -> {month_end} ===")
        
        url = f"{BASE_URL}/{API_URL}/ventes"
        params = {
            'password': API_PASSWORD,
            'Idnats': id_nat,
            'dt1': month_start,
            'dt2': month_end
        }
        
        print(f"[SALES DEBUG] URL: {url}")
        print(f"[SALES DEBUG] Paramètres: {params}")
        
        try:
            print(f"[SALES DEBUG] Lancement requête HTTP...")
            response = requests.get(url, params=params, timeout=180)
            
            print(f"[SALES DEBUG] Status Code: {response.status_code}")
            
            if response.status_code == 200:
                print(f"[SALES DEBUG] Réponse 200 OK - Parsing JSON...")
                try:
                    month_data = response.json()
                    print(f"[SALES DEBUG] JSON parsing OK")
                    print(f"[SALES DEBUG] Type données: {type(month_data)}")
                    
                    if isinstance(month_data, list):
                        print(f"[SALES DEBUG] Données = liste de {len(month_data)} éléments")
                        
                        if len(month_data) > 0:
                            # Analyser la structure du premier élément
                            first_item = month_data[0]
                            print(f"[SALES DEBUG] Premier élément type: {type(first_item)}")
                            
                            if isinstance(first_item, dict):
                                print(f"[SALES DEBUG] Clés premier élément: {list(first_item.keys())}")
                                
                                # Vérifier s'il y a une clé 'ventes' 
                                if 'ventes' in first_item:
                                    ventes_data = first_item['ventes']
                                    print(f"[SALES DEBUG] Ventes dans premier élément: {len(ventes_data)} transactions")
                                else:
                                    print(f"[SALES DEBUG] PAS de clé 'ventes' trouvée")
                        
                        # Compter réellement les ventes avant d'ajouter
                        if month_data:
                            total_ventes_ce_mois = 0
                            for item in month_data:
                                if isinstance(item, dict) and 'ventes' in item:
                                    total_ventes_ce_mois += len(item['ventes'])
                                    
                            if total_ventes_ce_mois > 0:
                                all_data.extend(month_data)
                                print(f"[SALES DEBUG] ✅ {len(month_data)} blocs ajoutés ({total_ventes_ce_mois} ventes)")
                            else:
                                print(f"[SALES DEBUG] ⚠️ {len(month_data)} blocs mais 0 ventes pour {month_start}")
                                
                            print(f"[SALES DEBUG] Total all_data maintenant: {len(all_data)} éléments")
                        else:
                            print(f"[SALES DEBUG] ⚠️ Liste vide retournée pour {month_start}")
                            
                    elif isinstance(month_data, dict):
                        print(f"[SALES DEBUG] Données = dictionnaire")
                        print(f"[SALES DEBUG] Clés: {list(month_data.keys())}")
                        
                        # Vérifier que le dictionnaire n'est pas vide
                        if month_data:
                            # Compter les ventes si c'est un dictionnaire direct
                            ventes_count = 0
                            if 'ventes' in month_data:
                                ventes_count = len(month_data['ventes'])
                                
                            if ventes_count > 0:
                                all_data.append(month_data)
                                print(f"[SALES DEBUG] ✅ Dictionnaire ajouté ({ventes_count} ventes)")
                            else:
                                print(f"[SALES DEBUG] ⚠️ Dictionnaire mais 0 ventes pour {month_start}")
                        else:
                            print(f"[SALES DEBUG] ⚠️ Dictionnaire vide retourné pour {month_start}")
                        
                    else:
                        print(f"[SALES DEBUG] ⚠️ Type de données inattendu: {type(month_data)}")
                        
                except ValueError as json_error:
                    print(f"[SALES DEBUG] ❌ Erreur parsing JSON: {json_error}")
                    print(f"[SALES DEBUG] Raw response (100 premiers chars): {response.text[:100]}")
                    
            elif response.status_code == 204:
                print(f"[SALES DEBUG] ⚠️ Status 204 - Pas de ventes pour {month_start}")
                
            elif response.status_code == 400:
                print(f"[SALES DEBUG] ❌ Status 400 - Erreur de requête pour {month_start}")
                print(f"[SALES DEBUG] Response text: {response.text[:200]}")
                
                # GESTION SPÉCIALE ERREUR 400 (retry avec date max)
                if "dt2 est postérieur" in response.text:
                    import re
                    match = re.search(r"dernière date disponible = (\d{4}-\d{2}-\d{2})", response.text)
                    if match:
                        max_date = match.group(1)
                        print(f"[SALES DEBUG] 🔄 Retry avec date max: {max_date}")
                        
                        params['dt2'] = max_date
                        retry_response = requests.get(url, params=params, timeout=180)
                        if retry_response.status_code == 200:
                            month_data = retry_response.json()
                            if month_data:
                                if isinstance(month_data, list):
                                    all_data.extend(month_data)
                                else:
                                    all_data.append(month_data)
                                print(f"[SALES DEBUG] ✅ Retry réussi pour {month_start}")
                
            else:
                print(f"[SALES DEBUG] ❌ Status {response.status_code}")
                print(f"[SALES DEBUG] Response text (200 premiers chars): {response.text[:200]}")
                
        except requests.exceptions.Timeout:
            print(f"[SALES DEBUG] ❌ Timeout (180s) pour {month_start}")
        except requests.exceptions.RequestException as req_error:
            print(f"[SALES DEBUG] ❌ Erreur requête pour {month_start}: {req_error}")
        except Exception as e:
            print(f"[SALES DEBUG] ❌ Erreur inattendue pour {month_start}: {e}")
            import traceback
            print(f"[SALES DEBUG] Traceback: {traceback.format_exc()}")
    
    # Résumé final
    print(f"[SALES DEBUG] === RÉSUMÉ FINAL ===")
    print(f"[SALES DEBUG] Total blocs all_data: {len(all_data)}")
    
    if all_data:
        print(f"[SALES DEBUG] Type all_data[0]: {type(all_data[0])}")
        if isinstance(all_data[0], dict):
            print(f"[SALES DEBUG] Clés all_data[0]: {list(all_data[0].keys())}")
            
        # Compter le total de ventes
        total_ventes = 0
        for item in all_data:
            if isinstance(item, dict) and 'ventes' in item:
                total_ventes += len(item['ventes'])
        
        print(f"[SALES DEBUG] Total ventes dans all_data: {total_ventes}")
        
        # DÉTAIL PAR BLOC pour comprendre la répartition
        for idx, item in enumerate(all_data):
            if isinstance(item, dict) and 'ventes' in item:
                ventes_count = len(item['ventes'])
                cip_pharma = item.get('cip_pharma', 'unknown')
                print(f"[SALES DEBUG] Bloc {idx}: cip_pharma={cip_pharma}, ventes={ventes_count}")
    else:
        print(f"[SALES DEBUG] ⚠️ all_data est vide!")
    
    return all_data if all_data else []



def process_sales_with_tva_update(pharmacy: Pharmacy, sales_data: dict) -> dict:
   """
   Version corrigée du traitement des ventes avec gestion des duplicatas
   CORRIGÉ: Agrégation par (code13_ref, date) au lieu de global
   """
   from data.models import InternalProduct, Sales, InventorySnapshot
   from django.db import transaction
   
   result = {
       'sales_processed': 0,
       'tva_updates': 0,
       'errors': [],
       'debug_info': {
           'total_ventes_received': 0,
           'total_lignes_received': 0,
           'products_not_found': 0,
           'snapshots_not_found': 0,
           'existing_sales_skipped': 0
       }
   }
   
   # Validation de la structure des données
   logger.info("Analyse de la structure des données")
   
   if not isinstance(sales_data, list):
       logger.error(f"sales_data n'est pas une liste: {type(sales_data)}")
       return result
   
   if len(sales_data) == 0:
       logger.warning("sales_data est une liste vide")
       return result
   
   logger.info(f"sales_data est une liste de {len(sales_data)} éléments")
   
   # Analyser le premier élément
   first_element = sales_data[0]
   logger.info(f"Premier élément clés: {list(first_element.keys())}")
   
   if 'ventes' not in first_element:
       logger.error("Pas de clé 'ventes' dans les données")
       return result
   
   pharmacy_sales = first_element['ventes']
   result['debug_info']['total_ventes_received'] = len(pharmacy_sales)
   
   # Compter le total de lignes
   total_lignes = sum(len(vente.get('lignes', [])) for vente in pharmacy_sales)
   result['debug_info']['total_lignes_received'] = total_lignes
   
   logger.info(f"{len(pharmacy_sales)} ventes reçues avec {total_lignes} lignes")
   
   # NOUVELLE AGRÉGATION PAR (code13_ref, date)
   aggregated_sales = {}  # Clé: (code13_ref, date_str)
   tva_updates_map = {}
   
   # Pré-traitement avec agrégation par jour
   for vente in pharmacy_sales:
       try:
           # Extraire la date de la vente
           date_facture = vente.get('dateFacture', '')
           if not date_facture:
               continue
               
           vente_date = date_facture.split('T')[0]  # Format YYYY-MM-DD
           
           # Traiter chaque ligne
           lignes = vente.get('lignes', [])
           for ligne in lignes:
               try:
                   # Extraire les données
                   prod_id = ligne.get('prodId')
                   code13_ref = ligne.get('code13Ref')
                   qte = ligne.get('qte', 0)
                   tva = ligne.get('tva')
                   
                   if not prod_id or not code13_ref:
                       continue
                   
                   # Clé composite pour agrégation
                   composite_key = (code13_ref, vente_date)
                   
                   # Agrégation des quantités par (produit, date)
                   if composite_key not in aggregated_sales:
                       aggregated_sales[composite_key] = {
                           'code13_ref': code13_ref,
                           'date': vente_date,
                           'total_qte': 0,
                           'tva': tva
                       }
                   
                   aggregated_sales[composite_key]['total_qte'] += qte
                   
                   # Stocker la TVA (dernière valeur rencontrée)
                   if tva is not None:
                       tva_updates_map[code13_ref] = tva
                       
               except Exception as e:
                   error_msg = f"Erreur ligne {prod_id}: {e}"
                   if len(result['errors']) < 10:
                       logger.error(error_msg)
                       result['errors'].append(error_msg)
                   continue
                   
       except Exception as e:
           error_msg = f"Erreur vente {vente.get('id', 'unknown')}: {e}"
           if len(result['errors']) < 10:
               logger.error(error_msg)
               result['errors'].append(error_msg)
           continue
   
   logger.info(f"Après agrégation: {len(aggregated_sales)} ventes uniques (produit+date)")
   
   # Traitement par batch pour éviter les transactions trop lourdes
   BATCH_SIZE = 1000
   tva_updates_count = 0
   products_not_found = 0
   snapshots_not_found = 0
   existing_sales_skipped = 0
   
   # Convertir en liste pour traitement par batch
   sales_list = list(aggregated_sales.values())
   
   # Traiter par batch
   for batch_start in range(0, len(sales_list), BATCH_SIZE):
       batch_end = min(batch_start + BATCH_SIZE, len(sales_list))
       batch_sales = sales_list[batch_start:batch_end]
       
       logger.info(f"Traitement batch {batch_start}-{batch_end}/{len(sales_list)}")
       
       with transaction.atomic():
           savepoint = transaction.savepoint()
           
           try:
               for sale_data in batch_sales:
                   try:
                       code13_ref = sale_data['code13_ref']
                       date_str = sale_data['date']
                       qte = sale_data['total_qte']
                       tva = sale_data.get('tva')
                       
                       # Recherche du produit interne
                       try:
                           internal_product = InternalProduct.objects.get(
                               pharmacy=pharmacy,
                               code_13_ref_id=code13_ref
                           )
                       except InternalProduct.DoesNotExist:
                           products_not_found += 1
                           if products_not_found <= 5:
                               logger.warning(f"Produit non trouvé: {code13_ref}")
                           continue
                       
                       # Recherche du snapshot
                       try:
                           latest_snapshot = InventorySnapshot.objects.filter(
                               product=internal_product
                           ).order_by('-date', '-created_at').first()
                           
                           if not latest_snapshot:
                               snapshots_not_found += 1
                               if snapshots_not_found <= 5:
                                   logger.warning(f"Pas de snapshot pour: {code13_ref}")
                               continue
                               
                       except Exception as e:
                           logger.error(f"Erreur recherche snapshot {code13_ref}: {e}")
                           continue
                       
                       # SOLUTION: get_or_create pour éviter les duplicatas
                       sale, created = Sales.objects.get_or_create(
                           date=date_str,
                           product_id=latest_snapshot.id,
                           defaults={'quantity': qte}
                       )
                       
                       if created:
                           result['sales_processed'] += 1
                       else:
                           # Si déjà existant, on additionne les quantités
                           sale.quantity += qte
                           sale.save()
                           existing_sales_skipped += 1
                       
                       # Mise à jour TVA
                       if tva is not None and internal_product.TVA != tva:
                           internal_product.TVA = tva
                           internal_product.save(update_fields=['TVA'])
                           tva_updates_count += 1
                           
                           if tva_updates_count <= 10:
                               logger.info(f"TVA mise à jour {code13_ref}: {tva}%")
                       
                   except Exception as e:
                       error_msg = f"Erreur traitement sale {sale_data}: {e}"
                       if len(result['errors']) < 10:
                           logger.error(error_msg)
                           result['errors'].append(error_msg)
                       continue
               
               # Si on arrive ici, le batch s'est bien passé
               transaction.savepoint_commit(savepoint)
               
           except Exception as e:
               # En cas d'erreur critique dans le batch, rollback et log
               transaction.savepoint_rollback(savepoint)
               logger.error(f"Erreur critique dans le batch {batch_start}-{batch_end}: {e}")
               result['errors'].append(f"Batch {batch_start}-{batch_end} échoué: {e}")
   
   # Mettre à jour les stats finales
   result['tva_updates'] = tva_updates_count
   result['debug_info']['products_not_found'] = products_not_found
   result['debug_info']['snapshots_not_found'] = snapshots_not_found
   result['debug_info']['existing_sales_skipped'] = existing_sales_skipped
   
   # Log final détaillé
   logger.info("RÉSULTAT FINAL TRAITEMENT VENTES:")
   logger.info(f"   Ventes reçues: {result['debug_info']['total_ventes_received']}")
   logger.info(f"   Lignes reçues: {result['debug_info']['total_lignes_received']}")
   logger.info(f"   Ventes agrégées: {len(sales_list)}")
   logger.info(f"   Ventes créées: {result['sales_processed']}")
   logger.info(f"   TVA mises à jour: {result['tva_updates']}")
   logger.info(f"   Produits non trouvés: {result['debug_info']['products_not_found']}")
   logger.info(f"   Snapshots manquants: {result['debug_info']['snapshots_not_found']}")
   logger.info(f"   Ventes existantes modifiées: {result['debug_info']['existing_sales_skipped']}")
   
   if result['debug_info']['products_not_found'] > 0:
       logger.warning(f"PROBLÈME: {result['debug_info']['products_not_found']} produits non trouvés")
       logger.warning("Vérifiez que les produits ont été importés avant les ventes")
   
   if result['debug_info']['snapshots_not_found'] > 0:
       logger.warning(f"PROBLÈME: {result['debug_info']['snapshots_not_found']} snapshots manquants")
   
   return result

@api_view(['POST'])
def create_pharmacy_products_and_sales_v2(request):
    """
    Endpoint combiné pour créer une pharmacie, récupérer ses produits ET ses ventes - Version 2
    POST /api/v2/pharmacy/create-products-sales
    Body: {
        "name": "Pharmacie Mouysset V2", 
        "id_nat": "832002810"
    }
    PÉRIODE VENTES FIXE: avril à août 2025
    """
    try:
        data = request.data
        pharmacy_name = data.get('name')
        id_nat = data.get('id_nat')
        
        if not pharmacy_name or not id_nat:
            return Response({
                'error': 'name et id_nat requis'
            }, status=400)
        
        logger.info(f"Traitement complet: {pharmacy_name} ({id_nat}) + ventes période fixe avril-août 2025")
        
        # === 1. CRÉER/RÉCUPÉRER LA PHARMACIE ===
        existing_pharmacy = None
        if Pharmacy.objects.filter(name=pharmacy_name).exists():
            existing_pharmacy = Pharmacy.objects.get(name=pharmacy_name)
            logger.info(f"Pharmacie trouvée par nom: {pharmacy_name}")
        elif Pharmacy.objects.filter(id_nat=id_nat).exists():
            existing_pharmacy = Pharmacy.objects.get(id_nat=id_nat)
            logger.info(f"Pharmacie trouvée par id_nat: {id_nat}")
        
        if existing_pharmacy:
            pharmacy = existing_pharmacy
            pharmacy_status = 'exists'
            pharmacy_message = f"Pharmacie '{pharmacy_name}' existe déjà"
        else:
            with transaction.atomic():
                pharmacy = Pharmacy.objects.create(
                    id=uuid.uuid4(),
                    name=pharmacy_name,
                    id_nat=id_nat,
                    address="Adresse import",
                    area="Région import",
                    ca=0,
                    employees_count=1
                )
            pharmacy_status = 'created'
            pharmacy_message = f"Pharmacie '{pharmacy_name}' créée avec succès"
            logger.info(f"Nouvelle pharmacie créée: {pharmacy_name} ({id_nat})")
        
        # === 2. RÉCUPÉRER ET TRAITER LES PRODUITS ===
        logger.info(f"Récupération des produits pour {pharmacy.name}")
        
        products_data = fetch_winpharma_products(pharmacy.id_nat)
        
        if products_data is None:
            products_result = {
                'message': 'Erreur lors de la récupération des produits',
                'status': 'error',
                'stats': {},
                'error': 'API WinPharma indisponible'
            }
        elif not products_data:
            products_result = {
                'message': 'Aucun produit trouvé',
                'status': 'success',
                'stats': {'products_processed': 0, 'snapshots_created': 0},
                'error': None
            }
        else:
            try:
                result = winpharma_new_api.process_product(pharmacy, products_data)
                products_result = {
                    'message': f"Produits traités avec succès",
                    'status': 'success',
                    'stats': {
                        'products_processed': len(result.get('products', [])),
                        'snapshots_created': len(result.get('snapshots', [])),
                        'raw_data_size': len(str(products_data))
                    },
                    'error': None
                }
            except Exception as e:
                logger.error(f"Erreur traitement produits: {e}")
                products_result = {
                    'message': 'Erreur lors du traitement des produits',
                    'status': 'error',
                    'stats': {},
                    'error': str(e)
                }
        
        # === 3. RÉCUPÉRER ET TRAITER LES VENTES (seulement si produits OK) ===
        sales_result = {
            'message': 'Ventes non traitées (produits requis)',
            'status': 'skipped',
            'stats': {},
            'error': 'Produits non disponibles'
        }
        
        if products_result['status'] == 'success':
            logger.info(f"Récupération des ventes pour {pharmacy.name} - Période FIXE: avril-août 2025")
            
            # NOUVELLE VERSION: sans paramètres de dates
            sales_data = fetch_winpharma_sales(pharmacy.id_nat)
            
            if sales_data is None:
                sales_result = {
                    'message': 'Erreur lors de la récupération des ventes',
                    'status': 'error',
                    'stats': {},
                    'error': 'API WinPharma indisponible'
                }
            elif not sales_data:
                sales_result = {
                    'message': 'Aucune vente trouvée pour la période avril-août 2025',
                    'status': 'success',
                    'stats': {'sales_processed': 0, 'tva_updates': 0},
                    'error': None
                }
            else:
                try:
                    winpharma_2.process_sales(pharmacy, sales_data)
                    sales_result = {
                        'message': f"Ventes traitées avec succès",
                        'status': 'success',
                        'stats': {
                            'raw_data_size': len(str(sales_data)),
                            'data_blocks': len(sales_data),
                            'period_note': 'Données récupérées mois par mois (avril-août 2025)'
                        },
                        'error': None
                    }
                except Exception as e:
                    logger.error(f"Erreur traitement ventes: {e}")
                    sales_result = {
                        'message': 'Erreur lors du traitement des ventes',
                        'status': 'error',
                        'stats': {},
                        'error': str(e)
                    }
        
        # === 4. CONSTRUIRE LA RÉPONSE COMBINÉE ===
        overall_status = 'success'
        if products_result['status'] == 'error':
            overall_status = 'partial_success' if pharmacy_status == 'created' else 'error'
        elif sales_result['status'] == 'error':
            overall_status = 'partial_success'
        
        combined_response = {
            'pharmacy': {
                'message': pharmacy_message,
                'pharmacy_id': str(pharmacy.id),
                'id_nat': pharmacy.id_nat,
                'status': pharmacy_status
            },
            'products': products_result,
            'sales': sales_result,
            'period': "2025-04-01 -> 2025-08-31 (période fixe)",
            'overall_status': overall_status
        }
        
        return Response(combined_response, status=200)
        
    except Exception as e:
        logger.error(f"Erreur create_pharmacy_products_and_sales_v2: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return Response({
            'error': 'Erreur serveur interne lors du traitement combiné',
            'details': str(e)
        }, status=500)

def fetch_winpharma_orders(id_nat: str, dt1: str, dt2: str) -> dict:
    """
    Récupère les achats depuis l'API WinPharma pour une pharmacie et une période
    FAIT DES APPELS MOIS PAR MOIS (avril à août 2025)
    VERSION CORRIGÉE INSPIRÉE DU SCRIPT HISTORIQUE
    """
    
    # Mois fixes à traiter 
    months = [
        ("2025-04-01", "2025-04-30"),
        ("2025-05-01", "2025-05-31"), 
        ("2025-06-01", "2025-06-30"),
        ("2025-07-01", "2025-07-31"),
        ("2025-08-01", "2025-08-31")
    ]
    
    all_data = []
    
    print(f"[ORDERS DEBUG] Début récupération achats pour {id_nat}")
    print(f"[ORDERS DEBUG] Paramètres reçus: dt1={dt1}, dt2={dt2} (IGNORÉS)")
    print(f"[ORDERS DEBUG] Traitement de {len(months)} mois fixes")
    
    for i, (month_start, month_end) in enumerate(months, 1):
        print(f"[ORDERS DEBUG] === MOIS {i}/5: {month_start} -> {month_end} ===")
        
        url = f"{BASE_URL}/{API_URL}/achats"
        params = {
            'password': API_PASSWORD,
            'Idnats': id_nat,
            'dt1': month_start,
            'dt2': month_end
        }
        
        print(f"[ORDERS DEBUG] URL: {url}")
        print(f"[ORDERS DEBUG] Paramètres: {params}")
        
        try:
            print(f"[ORDERS DEBUG] Lancement requête HTTP...")
            response = requests.get(url, params=params, timeout=180)
            
            print(f"[ORDERS DEBUG] Status Code: {response.status_code}")
            
            if response.status_code == 200:
                print(f"[ORDERS DEBUG] Réponse 200 OK - Parsing JSON...")
                try:
                    month_data = response.json()
                    print(f"[ORDERS DEBUG] JSON parsing OK")
                    print(f"[ORDERS DEBUG] Type données: {type(month_data)}")
                    
                    if isinstance(month_data, list):
                        print(f"[ORDERS DEBUG] Données = liste de {len(month_data)} éléments")
                        
                        if len(month_data) > 0:
                            # Analyser la structure du premier élément
                            first_item = month_data[0]
                            print(f"[ORDERS DEBUG] Premier élément type: {type(first_item)}")
                            
                            if isinstance(first_item, dict):
                                print(f"[ORDERS DEBUG] Clés premier élément: {list(first_item.keys())}")
                                
                                # Vérifier s'il y a une clé 'achats' 
                                if 'achats' in first_item:
                                    achats_data = first_item['achats']
                                    print(f"[ORDERS DEBUG] Achats dans premier élément: {len(achats_data)} commandes")
                                else:
                                    print(f"[ORDERS DEBUG] PAS de clé 'achats' trouvée")
                        
                        # 🔧 CORRECTION: Compter réellement les achats avant d'ajouter
                        if month_data:
                            # Compter les achats dans cette réponse
                            total_achats_ce_mois = 0
                            for item in month_data:
                                if isinstance(item, dict) and 'achats' in item:
                                    total_achats_ce_mois += len(item['achats'])
                                    
                            if total_achats_ce_mois > 0:
                                all_data.extend(month_data)
                                print(f"[ORDERS DEBUG] ✅ {len(month_data)} blocs ajoutés ({total_achats_ce_mois} commandes)")
                            else:
                                print(f"[ORDERS DEBUG] ⚠️ {len(month_data)} blocs mais 0 commandes pour {month_start}")
                                
                            print(f"[ORDERS DEBUG] Total all_data maintenant: {len(all_data)} éléments")
                        else:
                            print(f"[ORDERS DEBUG] ⚠️ Liste vide retournée pour {month_start}")
                            
                    elif isinstance(month_data, dict):
                        print(f"[ORDERS DEBUG] Données = dictionnaire")
                        print(f"[ORDERS DEBUG] Clés: {list(month_data.keys())}")
                        
                        # 🔧 CORRECTION: Vérifier que le dictionnaire n'est pas vide
                        if month_data:
                            # Compter les achats si c'est un dictionnaire direct
                            achats_count = 0
                            if 'achats' in month_data:
                                achats_count = len(month_data['achats'])
                                
                            if achats_count > 0:
                                all_data.append(month_data)
                                print(f"[ORDERS DEBUG] ✅ Dictionnaire ajouté ({achats_count} commandes)")
                            else:
                                print(f"[ORDERS DEBUG] ⚠️ Dictionnaire mais 0 commandes pour {month_start}")
                        else:
                            print(f"[ORDERS DEBUG] ⚠️ Dictionnaire vide retourné pour {month_start}")
                        
                    else:
                        print(f"[ORDERS DEBUG] ⚠️ Type de données inattendu: {type(month_data)}")
                        
                except ValueError as json_error:
                    print(f"[ORDERS DEBUG] ❌ Erreur parsing JSON: {json_error}")
                    print(f"[ORDERS DEBUG] Raw response (100 premiers chars): {response.text[:100]}")
                    
            elif response.status_code == 204:
                print(f"[ORDERS DEBUG] ⚠️ Status 204 - Pas d'achats pour {month_start}")
                
            elif response.status_code == 400:
                print(f"[ORDERS DEBUG] ❌ Status 400 - Erreur de requête pour {month_start}")
                print(f"[ORDERS DEBUG] Response text: {response.text[:200]}")
                
                # 🆕 GESTION SPÉCIALE ERREUR 400 (comme dans le script historique)
                if "dt2 est postérieur" in response.text:
                    import re
                    match = re.search(r"dernière date disponible = (\d{4}-\d{2}-\d{2})", response.text)
                    if match:
                        max_date = match.group(1)
                        print(f"[ORDERS DEBUG] 🔄 Retry avec date max: {max_date}")
                        
                        params['dt2'] = max_date
                        retry_response = requests.get(url, params=params, timeout=180)
                        if retry_response.status_code == 200:
                            month_data = retry_response.json()
                            if month_data:
                                if isinstance(month_data, list):
                                    all_data.extend(month_data)
                                else:
                                    all_data.append(month_data)
                                print(f"[ORDERS DEBUG] ✅ Retry réussi pour {month_start}")
                
            else:
                print(f"[ORDERS DEBUG] ❌ Status {response.status_code}")
                print(f"[ORDERS DEBUG] Response text (200 premiers chars): {response.text[:200]}")
                
        except requests.exceptions.Timeout:
            print(f"[ORDERS DEBUG] ❌ Timeout (180s) pour {month_start}")
        except requests.exceptions.RequestException as req_error:
            print(f"[ORDERS DEBUG] ❌ Erreur requête pour {month_start}: {req_error}")
        except Exception as e:
            print(f"[ORDERS DEBUG] ❌ Erreur inattendue pour {month_start}: {e}")
            import traceback
            print(f"[ORDERS DEBUG] Traceback: {traceback.format_exc()}")
    
    # Résumé final
    print(f"[ORDERS DEBUG] === RÉSUMÉ FINAL ===")
    print(f"[ORDERS DEBUG] Total blocs all_data: {len(all_data)}")
    
    if all_data:
        print(f"[ORDERS DEBUG] Type all_data[0]: {type(all_data[0])}")
        if isinstance(all_data[0], dict):
            print(f"[ORDERS DEBUG] Clés all_data[0]: {list(all_data[0].keys())}")
            
        # Compter le total d'achats
        total_achats = 0
        for item in all_data:
            if isinstance(item, dict) and 'achats' in item:
                total_achats += len(item['achats'])
        
        print(f"[ORDERS DEBUG] Total commandes dans all_data: {total_achats}")
        
        # 🆕 DÉTAIL PAR BLOC pour comprendre la répartition
        for idx, item in enumerate(all_data):
            if isinstance(item, dict) and 'achats' in item:
                achats_count = len(item['achats'])
                cip_pharma = item.get('cip_pharma', 'unknown')
                print(f"[ORDERS DEBUG] Bloc {idx}: cip_pharma={cip_pharma}, achats={achats_count}")
    else:
        print(f"[ORDERS DEBUG] ⚠️ all_data est vide!")
    
    return all_data if all_data else []

@api_view(['POST'])
def fetch_and_process_orders_v2(request):
    """
    Endpoint pour récupérer et traiter les achats d'une pharmacie - Version 2
    UTILISE MAINTENANT winpharma_historical
    POST /api/v2/pharmacy/orders/fetch
    Body: {"id_nat": "832002810", "dt1": "2025-04-01", "dt2": "2025-08-31"}
    """
    try:
        data = request.data
        id_nat = data.get('id_nat')
        pharmacy_name = data.get('pharmacy_name')
        dt1 = data.get('dt1')
        dt2 = data.get('dt2')
        
        if not dt1 or not dt2:
            return Response({
                'error': 'dt1 et dt2 (dates) sont requis pour les achats'
            }, status=400)
        
        # Récupérer la pharmacie
        pharmacy = None
        if id_nat:
            try:
                pharmacy = Pharmacy.objects.get(id_nat=id_nat)
            except Pharmacy.DoesNotExist:
                return Response({
                    'error': f'Pharmacie avec id_nat {id_nat} non trouvée'
                }, status=404)
        elif pharmacy_name:
            try:
                pharmacy = Pharmacy.objects.get(name=pharmacy_name)
            except Pharmacy.DoesNotExist:
                return Response({
                    'error': f'Pharmacie avec nom {pharmacy_name} non trouvée'
                }, status=404)
        else:
            return Response({
                'error': 'id_nat ou pharmacy_name requis'
            }, status=400)
        
        logger.info(f"Récupération des achats pour {pharmacy.name} ({pharmacy.id_nat}) - Période: {dt1} -> {dt2}")
        
        # Récupérer les données d'achats de l'API WinPharma
        orders_data = fetch_winpharma_orders(pharmacy.id_nat, dt1, dt2)
        
        if orders_data is None:
            return Response({
                'error': 'Erreur lors de la récupération des achats depuis l\'API'
            }, status=500)
        
        if not orders_data:
            return Response({
                'message': 'Aucun achat trouvé pour cette période',
                'pharmacy_id': str(pharmacy.id),
                'pharmacy_name': pharmacy.name,
                'period': f"{dt1} -> {dt2}",
                'orders_count': 0
            }, status=200)
        
        # 🔧 NOUVEAU: Traiter les achats avec winpharma_historical au lieu de winpharma_2
        try:
            from data.services import winpharma_historical
            
            result = winpharma_historical.process_order(pharmacy, orders_data)
            
            return Response({
                'message': f"Achats traités avec succès pour {pharmacy.name}",
                'pharmacy_id': str(pharmacy.id),
                'pharmacy_name': pharmacy.name,
                'id_nat': pharmacy.id_nat,
                'period': f"{dt1} -> {dt2}",
                'stats': {
                    'suppliers_processed': len(result.get('suppliers', [])),
                    'products_processed': len(result.get('products', [])),
                    'orders_processed': len(result.get('orders', [])),
                    'product_orders_processed': len(result.get('product_orders', [])),
                    'raw_data_size': len(str(orders_data))
                },
                'status': 'success'
            }, status=200)
            
        except Exception as e:
            logger.error(f"Erreur traitement achats: {e}")
            return Response({
                'error': f'Erreur lors du traitement des achats: {str(e)}',
                'pharmacy_id': str(pharmacy.id),
                'pharmacy_name': pharmacy.name
            }, status=500)
        
    except Exception as e:
        logger.error(f"Erreur générale fetch_and_process_orders_v2: {e}")
        return Response({
            'error': 'Erreur serveur interne'
        }, status=500)