import traceback
import requests
import logging
from datetime import datetime, timedelta

from rest_framework.decorators import api_view
from rest_framework.response import Response

from data.models import Pharmacy
from data.services import dexter, winpharma, winpharma_2, winpharma_new_api

from data.services import dexter, winpharma, winpharma_2, winpharma_historical

from data.services import apothical

from data.models import Pharmacy
from rest_framework.decorators import api_view
from rest_framework.response import Response
import json
import uuid
from django.db import transaction


logger = logging.getLogger(__name__)

# Credentials de test pour nouvelle API
API_URL = "YXBvdGhpY2Fs"
API_PASSWORD = "cGFzczE"
IDNAT_TEST = "062044623"
BASE_URL = "https://grpstat.winpharma.com/ApiWp"


# ============================================================================
# ENDPOINTS EXISTANTS (inchangés)
# ============================================================================

@api_view(['POST'])
def winpharma_create_product(request):
    """
    Endpoint for creating or updating products linked to a pharmacy.
    """
    pharmacy, _ = Pharmacy.objects.get_or_create(id_nat=request.headers.get('Pharmacy-id'))

    try:
        winpharma.process_product(pharmacy, request.data)
    except Exception as e:
        print(traceback.format_exc())
        return Response({
            "message": "Processing error",
        }, status=500)

    return Response({
        "message": "Processing completed",
    }, status=200)


@api_view(['POST'])
def winpharma_create_order(request):
    """
    Endpoint for creating or updating orders linked to a pharmacy.
    """
    pharmacy, _ = Pharmacy.objects.get_or_create(id_nat=request.headers.get('Pharmacy-id'))
    try:
        winpharma.process_order(pharmacy, request.data)
    except Exception as e:
        print(traceback.format_exc())
        return Response({
            "message": "Processing error",
        }, status=500)

    return Response({
        "message": "Processing completed",
    }, status=200)


@api_view(['POST'])
def winpharma_create_sales(request):
    """
    Endpoint for creating or updating sales linked to a pharmacy.
    """
    pharmacy, _ = Pharmacy.objects.get_or_create(id_nat=request.headers.get('Pharmacy-id'))
    try:
        winpharma.process_sales(pharmacy, request.data)
    except Exception as e:
        print(traceback.format_exc())
        return Response({
            "message": "Processing error",
        }, status=500)

    return Response({
        "message": "Processing completed",
    }, status=200)


@api_view(['POST'])
def dexter_create_stock(request):
    """
    Endpoint for creating or updating products linked to a pharmacy.
    """
    orga = request.data['organization']
    pharmacy, _ = Pharmacy.objects.update_or_create(id_nat=orga['id_national'],
                                                    defaults={'name': orga['nom_pharmacie']})
    try:
        dexter.process_stock(pharmacy, request.data['produits'], orga['date_fichier'])
    except Exception as e:
        print(traceback.format_exc())
        return Response({
            "message": "Processing error",
        }, status=500)

    return Response({
        "message": "Processing completed",
    }, status=200)


@api_view(['POST'])
def dexter_create_achat(request):
    """
    Endpoint for creating or updating orders linked to a pharmacy.
    """
    orga = request.data['organization']
    pharmacy, _ = Pharmacy.objects.update_or_create(id_nat=orga['id_national'],
                                                    defaults={'name': orga['nom_pharmacie']})

    try:
        dexter.process_achat(pharmacy, request.data['achats'])
    except Exception as e:
        print(traceback.format_exc())
        return Response({
            "message": "Processing error",
        }, status=500)

    return Response({
        "message": "Processing completed",
    }, status=200)


@api_view(['POST'])
def dexter_create_vente(request):
    """
    Endpoint for creating or updating sales linked to a pharmacy.
    """
    orga = request.data['organization']
    pharmacy, _ = Pharmacy.objects.update_or_create(id_nat=orga['id_national'],
                                                    defaults={'name': orga['nom_pharmacie']})

    try:
        dexter.process_vente(pharmacy, request.data['ventes'])
    except Exception as e:
        print(traceback.format_exc())
        return Response({
            "message": "Processing error",
        }, status=500)

    return Response({
        "message": "Processing completed",
    }, status=200)


@api_view(['POST'])
def winpharma_2_create_product(request):
    """
    Endpoint for creating or updating products linked to a pharmacy.
    """
    pharmacy, _ = Pharmacy.objects.get_or_create(id_nat=request.headers.get('Pharmacy-id'))

    try:
        winpharma_2.process_product(pharmacy, request.data)
    except Exception as e:
        print(traceback.format_exc())
        return Response({
            "message": "Processing error",
        }, status=500)

    return Response({
        "message": "Processing completed",
    }, status=200)


@api_view(['POST'])
def winpharma_2_create_order(request):
    """
    Endpoint for creating or updating orders linked to a pharmacy.
    """
    pharmacy, _ = Pharmacy.objects.get_or_create(id_nat=request.headers.get('Pharmacy-id'))
    try:
        winpharma_2.process_order(pharmacy, request.data)
    except Exception as e:
        print(traceback.format_exc())
        return Response({
            "message": "Processing error",
        }, status=500)

    return Response({
        "message": "Processing completed",
    }, status=200)


@api_view(['POST'])
def winpharma_2_create_sales(request):
    """
    Endpoint for creating or updating sales linked to a pharmacy.
    """
    pharmacy, _ = Pharmacy.objects.get_or_create(id_nat=request.headers.get('Pharmacy-id'))
    try:
        winpharma_2.process_sales(pharmacy, request.data)
    except Exception as e:
        print(traceback.format_exc())
        return Response({
            "message": "Processing error",
        }, status=500)

    return Response({
        "message": "Processing completed",
    }, status=200)


# ============================================================================
# ENDPOINTS PRODUCTION NOUVELLE API
# ============================================================================

@api_view(['POST'])
def winpharma_new_api_create_product(request):
    """
    Endpoint de production pour créer/mettre à jour les produits avec la nouvelle API
    """
    pharmacy, _ = Pharmacy.objects.get_or_create(id_nat=request.headers.get('Pharmacy-id'))

    try:
        result = winpharma_new_api.process_product(pharmacy, request.data)
        return Response({
            "message": "Products processed successfully",
            "stats": {
                "products_processed": len(result.get('products', [])),
                "snapshots_created": len(result.get('snapshots', []))
            }
        }, status=200)
    except Exception as e:
        print(traceback.format_exc())
        return Response({
            "message": "Processing error",
            "error": str(e)
        }, status=500)


@api_view(['POST'])
def winpharma_new_api_create_order(request):
    """
    Endpoint de production pour créer/mettre à jour les commandes avec la nouvelle API
    """
    pharmacy, _ = Pharmacy.objects.get_or_create(id_nat=request.headers.get('Pharmacy-id'))
    
    try:
        result = winpharma_new_api.process_order(pharmacy, request.data)
        return Response({
            "message": "Orders processed successfully",
            "stats": {
                "suppliers_processed": len(result.get('suppliers', [])),
                "products_processed": len(result.get('products', [])),
                "orders_processed": len(result.get('orders', [])),
                "product_orders_processed": len(result.get('product_orders', []))
            }
        }, status=200)
    except Exception as e:
        print(traceback.format_exc())
        return Response({
            "message": "Processing error",
            "error": str(e)
        }, status=500)


@api_view(['POST'])
def winpharma_new_api_create_sales(request):
    """
    Endpoint de production pour créer/mettre à jour les ventes avec la nouvelle API
    """
    pharmacy, _ = Pharmacy.objects.get_or_create(id_nat=request.headers.get('Pharmacy-id'))
    
    try:
        winpharma_new_api.process_sales(pharmacy, request.data)
        return Response({
            "message": "Sales processed successfully"
        }, status=200)
    except Exception as e:
        print(traceback.format_exc())
        return Response({
            "message": "Processing error",
            "error": str(e)
        }, status=500)


# ============================================================================
# NOUVEAUX ENDPOINTS POUR TESTS NOUVELLE API
# ============================================================================

def fetch_from_new_api(endpoint, params=None):
    """
    Fonction utilitaire pour récupérer des données de la nouvelle API
    """
    url = f"{BASE_URL}/{API_URL}/{endpoint}"
    base_params = {
        'password': API_PASSWORD,
        'Idnats': IDNAT_TEST
    }
    if params:
        base_params.update(params)
    
    try:
        response = requests.get(url, params=base_params, timeout=30)
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"API Error {response.status_code}: {response.text}")
            return None
    except Exception as e:
        logger.error(f"Request failed: {e}")
        return None


@api_view(['POST'])
def test_new_api_products(request):
    """
    Test endpoint pour tester le traitement des produits avec la nouvelle API
    
    Usage: POST /test/new_api/products
    """
    logger.info("🧪 Testing new API products endpoint")
    
    # Récupérer les données de la nouvelle API
    data = fetch_from_new_api("produits")
    
    if not data:
        return Response({
            "error": "Failed to fetch data from new API"
        }, status=500)
    
    # Récupérer ou créer la pharmacie de test
    pharmacy, created = Pharmacy.objects.get_or_create(
        id_nat=IDNAT_TEST,
        defaults={'name': 'Pharmacie Test API'}
    )
    
    try:
        # Tester notre service avec les vraies données
        result = winpharma_new_api.process_product(pharmacy, data)
        
        return Response({
            "status": "success",
            "message": "Products processed successfully",
            "stats": {
                "products_processed": len(result.get('products', [])),
                "snapshots_created": len(result.get('snapshots', [])),
                "pharmacy_id": str(pharmacy.id),
                "pharmacy_name": pharmacy.name,
                "data_source": "new_api",
                "pharmacy_created": created
            }
        }, status=200)
        
    except Exception as e:
        logger.error(f"Error processing products: {e}")
        return Response({
            "error": f"Processing error: {str(e)}",
            "traceback": traceback.format_exc()
        }, status=500)


@api_view(['POST'])
def test_new_api_orders(request):
    """
    Test endpoint pour tester le traitement des commandes avec la nouvelle API
    
    Usage: POST /test/new_api/orders
    """
    logger.info("🧪 Testing new API orders endpoint")
    
    # Récupérer les données avec période récente
    dt2 = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    dt1 = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    
    params = {'dt1': dt1, 'dt2': dt2}
    data = fetch_from_new_api("achats", params)
    
    if not data:
        return Response({
            "error": "Failed to fetch data from new API"
        }, status=500)
    
    # Récupérer ou créer la pharmacie de test
    pharmacy, created = Pharmacy.objects.get_or_create(
        id_nat=IDNAT_TEST,
        defaults={'name': 'Pharmacie Test API'}
    )
    
    try:
        # Tester notre service avec les vraies données
        result = winpharma_new_api.process_order(pharmacy, data)
        
        return Response({
            "status": "success",
            "message": "Orders processed successfully",
            "stats": {
                "suppliers_processed": len(result.get('suppliers', [])),
                "products_processed": len(result.get('products', [])),
                "orders_processed": len(result.get('orders', [])),
                "product_orders_processed": len(result.get('product_orders', [])),
                "pharmacy_id": str(pharmacy.id),
                "pharmacy_name": pharmacy.name,
                "period": f"{dt1} -> {dt2}",
                "data_source": "new_api",
                "pharmacy_created": created
            }
        }, status=200)
        
    except Exception as e:
        logger.error(f"Error processing orders: {e}")
        return Response({
            "error": f"Processing error: {str(e)}",
            "traceback": traceback.format_exc()
        }, status=500)


@api_view(['POST'])
def test_new_api_sales(request):
    """
    Test endpoint pour tester le traitement des ventes avec la nouvelle API
    
    Usage: POST /test/new_api/sales
    """
    logger.info("🧪 Testing new API sales endpoint")
    
    # Utiliser la période connue qui marche
    params = {'dt1': '2025-05-20', 'dt2': '2025-05-24'}
    data = fetch_from_new_api("ventes", params)
    
    if not data:
        return Response({
            "error": "Failed to fetch data from new API"
        }, status=500)
    
    # Récupérer ou créer la pharmacie de test
    pharmacy, created = Pharmacy.objects.get_or_create(
        id_nat=IDNAT_TEST,
        defaults={'name': 'Pharmacie Test API'}
    )
    
    try:
        # Tester notre service avec les vraies données
        winpharma_new_api.process_sales(pharmacy, data)
        
        return Response({
            "status": "success",
            "message": "Sales processed successfully",
            "stats": {
                "pharmacy_id": str(pharmacy.id),
                "pharmacy_name": pharmacy.name,
                "period": "2025-05-20 -> 2025-05-24",
                "data_source": "new_api",
                "pharmacy_created": created
            }
        }, status=200)
        
    except Exception as e:
        logger.error(f"Error processing sales: {e}")
        return Response({
            "error": f"Processing error: {str(e)}",
            "traceback": traceback.format_exc()
        }, status=500)


@api_view(['GET'])
def test_new_api_summary(request):
    """
    Endpoint pour avoir un résumé des données de la pharmacie de test
    
    Usage: GET /test/new_api/summary
    """
    try:
        pharmacy = Pharmacy.objects.get(id_nat=IDNAT_TEST)
        
        # Compter les données
        from data.models import InternalProduct, Order, Sales, InventorySnapshot
        
        products_count = InternalProduct.objects.filter(pharmacy=pharmacy).count()
        orders_count = Order.objects.filter(pharmacy=pharmacy).count()
        sales_count = Sales.objects.filter(product__product__pharmacy=pharmacy).count()
        snapshots_count = InventorySnapshot.objects.filter(product__pharmacy=pharmacy).count()
        
        # Dernières données
        latest_snapshot = InventorySnapshot.objects.filter(
            product__pharmacy=pharmacy
        ).order_by('-created_at').first()
        
        latest_order = Order.objects.filter(pharmacy=pharmacy).order_by('-created_at').first()
        
        latest_sale = Sales.objects.filter(
            product__product__pharmacy=pharmacy
        ).order_by('-created_at').first()
        
        return Response({
            "pharmacy": {
                "id": str(pharmacy.id),
                "id_nat": pharmacy.id_nat,
                "name": pharmacy.name,
                "created_at": pharmacy.created_at,
                "updated_at": pharmacy.updated_at
            },
            "stats": {
                "products": products_count,
                "orders": orders_count,
                "sales": sales_count,
                "snapshots": snapshots_count
            },
            "latest_data": {
                "latest_snapshot_date": latest_snapshot.date if latest_snapshot else None,
                "latest_snapshot_created": latest_snapshot.created_at if latest_snapshot else None,
                "latest_order_date": latest_order.created_at if latest_order else None,
                "latest_sale_date": latest_sale.created_at if latest_sale else None
            }
        })
        
    except Pharmacy.DoesNotExist:
        return Response({
            "error": "Test pharmacy not found. Run a test endpoint first.",
            "suggestion": "POST /test/new_api/products to create test data"
        }, status=404)
    except Exception as e:
        return Response({
            "error": f"Error: {str(e)}",
            "traceback": traceback.format_exc()
        }, status=500)

# ========================================
# AJOUT DANS data/views.py
# ========================================

# Ajouter cet import en haut du fichier

# Ajouter ces 3 nouvelles vues à la fin du fichier

def winpharma_historical_create_product(request):
    """
    Endpoint for creating or updating products linked to a pharmacy - HISTORICAL IMPORT VERSION.
    Accepte Pharmacy-Name OU Pharmacy-id
    """
    pharmacy = get_pharmacy_from_request(request)
    if not pharmacy:
        return Response({
            "message": "Header Pharmacy-Name ou Pharmacy-id requis",
        }, status=400)

    try:
        result = winpharma_historical.process_product(pharmacy, request.data)
        return Response({
            "message": f"Products processed for {pharmacy.name}",
            "pharmacy_id": str(pharmacy.id)
        }, status=200)
    except Exception as e:
        print(traceback.format_exc())
        return Response({
            "message": "Processing error",
        }, status=500)


@api_view(['POST'])
def winpharma_historical_create_order(request):
    """
    Endpoint for creating or updating orders linked to a pharmacy - HISTORICAL IMPORT VERSION.
    Accepte Pharmacy-Name OU Pharmacy-id
    """
    pharmacy = get_pharmacy_from_request(request)
    if not pharmacy:
        return Response({
            "message": "Header Pharmacy-Name ou Pharmacy-id requis",
        }, status=400)
    
    try:
        result = winpharma_historical.process_order(pharmacy, request.data)
        return Response({
            "message": f"Orders processed for {pharmacy.name}",
            "pharmacy_id": str(pharmacy.id)
        }, status=200)
    except Exception as e:
        print(traceback.format_exc())
        return Response({
            "message": "Processing error",
        }, status=500)


@api_view(['POST'])
def winpharma_historical_create_sales(request):
    """
    Endpoint for creating or updating sales linked to a pharmacy - HISTORICAL IMPORT VERSION.
    Accepte Pharmacy-Name OU Pharmacy-id
    """
    pharmacy = get_pharmacy_from_request(request)
    if not pharmacy:
        return Response({
            "message": "Header Pharmacy-Name ou Pharmacy-id requis",
        }, status=400)
    
    try:
        winpharma_historical.process_sales(pharmacy, request.data)
        return Response({
            "message": f"Sales processed for {pharmacy.name}",
            "pharmacy_id": str(pharmacy.id)
        }, status=200)
    except Exception as e:
        print(traceback.format_exc())
        return Response({
            "message": "Processing error",
        }, status=500)

# Ajouter ces imports en haut du fichier data/views.py

# Ajouter ces vues à la fin du fichier data/views.py

@api_view(['POST'])
def apothical_create_product(request):
    """
    Endpoint pour créer/mettre à jour les produits Apothical
    """
    # Récupération du FINESS depuis les headers
    finess = request.headers.get('Pharmacy-Finess')
    if not finess:
        return Response({
            "message": "Header 'Pharmacy-Finess' requis",
        }, status=400)
    
    # Récupération ou création de la pharmacie
    pharmacy, created = Pharmacy.objects.get_or_create(
        id_nat=finess,
        defaults={'name': f'Pharmacie Apothical {finess}'}
    )
    
    if created:
        logger.info(f"Nouvelle pharmacie créée: {finess}")
    
    try:
        result = apothical.process_products(pharmacy, finess)
        
        return Response({
            "message": "Traitement produits Apothical terminé",
            "products_count": len(result.get('products', [])),
            "snapshots_count": len(result.get('snapshots', [])),
        }, status=200)
        
    except Exception as e:
        logger.error(f"Erreur traitement produits Apothical: {e}")
        return Response({
            "message": "Erreur lors du traitement des produits",
            "error": str(e)
        }, status=500)


@api_view(['POST'])
def apothical_create_order(request):
    """
    Endpoint pour créer/mettre à jour les commandes Apothical
    """
    # Récupération du FINESS depuis les headers
    finess = request.headers.get('Pharmacy-Finess')
    if not finess:
        return Response({
            "message": "Header 'Pharmacy-Finess' requis",
        }, status=400)
    
    # Récupération ou création de la pharmacie
    pharmacy, created = Pharmacy.objects.get_or_create(
        id_nat=finess,
        defaults={'name': f'Pharmacie Apothical {finess}'}
    )
    
    try:
        result = apothical.process_orders(pharmacy, finess)
        
        return Response({
            "message": "Traitement commandes Apothical terminé",
            "suppliers_count": len(result.get('suppliers', [])),
            "products_count": len(result.get('products', [])),
            "orders_count": len(result.get('orders', [])),
            "product_orders_count": len(result.get('product_orders', [])),
        }, status=200)
        
    except Exception as e:
        logger.error(f"Erreur traitement commandes Apothical: {e}")
        return Response({
            "message": "Erreur lors du traitement des commandes",
            "error": str(e)
        }, status=500)


@api_view(['POST'])
def apothical_create_sales(request):
    """
    Endpoint pour créer/mettre à jour les ventes Apothical
    """
    # Récupération du FINESS depuis les headers
    finess = request.headers.get('Pharmacy-Finess')
    if not finess:
        return Response({
            "message": "Header 'Pharmacy-Finess' requis",
        }, status=400)
    
    # Récupération ou création de la pharmacie
    pharmacy, created = Pharmacy.objects.get_or_create(
        id_nat=finess,
        defaults={'name': f'Pharmacie Apothical {finess}'}
    )
    
    try:
        apothical.process_sales(pharmacy, finess)
        
        return Response({
            "message": "Traitement ventes Apothical terminé",
        }, status=200)
        
    except Exception as e:
        logger.error(f"Erreur traitement ventes Apothical: {e}")
        return Response({
            "message": "Erreur lors du traitement des ventes",
            "error": str(e)
        }, status=500)
# ========================================
# AJOUT DANS data/urls.py 
# ========================================

# Ajouter ces 3 nouvelles routes dans la liste urlpatterns

# Ajouter cette fonction à la fin du fichier data/views.py
@api_view(['POST'])
def create_pharmacy(request):
    """
    Endpoint pour créer une nouvelle pharmacie
    POST /api/pharmacy/create
    Body: {"name": "Pharmacie Mouysset V2"}
    """
    try:
        data = request.data
        pharmacy_name = data.get('name')
        
        if not pharmacy_name:
            return Response({
                'error': 'Nom de pharmacie requis'
            }, status=400)
        
        # Vérifier si la pharmacie existe déjà
        if Pharmacy.objects.filter(name=pharmacy_name).exists():
            existing_pharmacy = Pharmacy.objects.get(name=pharmacy_name)
            return Response({
                'message': f"Pharmacie '{pharmacy_name}' existe déjà",
                'pharmacy_id': str(existing_pharmacy.id),
                'status': 'exists'
            }, status=200)
        
        # Créer la nouvelle pharmacie
        with transaction.atomic():
            pharmacy = Pharmacy.objects.create(
                id=uuid.uuid4(),
                name=pharmacy_name,
                id_nat=f"TEMP_{uuid.uuid4().hex[:8].upper()}",
                address="Adresse temporaire",
                area="Région temporaire",
                ca=0,
                employees_count=1
            )
            
        return Response({
            'message': f"Pharmacie '{pharmacy_name}' créée avec succès",
            'pharmacy_id': str(pharmacy.id),
            'status': 'created'
        }, status=201)
        
    except Exception as e:
        logger.error(f"Erreur création pharmacie: {e}")
        return Response({
            'error': 'Erreur serveur interne'
        }, status=500)

# Fonction helper pour gérer les headers
def get_pharmacy_from_request(request):
    """
    Récupère la pharmacie depuis le header Pharmacy-Name ou Pharmacy-id
    """
    # Priorité 1: Header Pharmacy-Name (pour pharmacies V2)
    pharmacy_name = request.headers.get('Pharmacy-Name')
    if pharmacy_name:
        try:
            pharmacy = Pharmacy.objects.get(name=pharmacy_name)
            return pharmacy
        except Pharmacy.DoesNotExist:
            return None
    
    # Priorité 2: Header Pharmacy-id (ancien système)
    pharmacy_id = request.headers.get('Pharmacy-id')
    if pharmacy_id:
        try:
            pharmacy, _ = Pharmacy.objects.get_or_create(id_nat=pharmacy_id)
            return pharmacy
        except Exception:
            return None
    
    return None