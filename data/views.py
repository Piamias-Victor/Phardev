import traceback

from rest_framework.decorators import api_view
from rest_framework.response import Response

from data.models import (Pharmacy)
from data.utils.process import (process_product_winpharma, process_order_winpharma, process_sales_winpharma,
                                process_stock_dexter, process_achat_dexter, process_vente_dexter)


@api_view(['POST'])
def winpharma_create_product(request):
    """
    Endpoint for creating or updating products linked to a pharmacy.
    """
    pharmacy, _ = Pharmacy.objects.get_or_create(id_nat=request.headers.get('Pharmacy-id'))

    try:
        process_product_winpharma(pharmacy, request.data)
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
        process_order_winpharma(pharmacy, request.data)
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
        process_sales_winpharma(pharmacy, request.data)
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
        process_stock_dexter(pharmacy, request.data['produits'], orga['date_fichier'])
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
        process_achat_dexter(pharmacy, request.data['achats'])
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
        process_vente_dexter(pharmacy, request.data['ventes'])
    except Exception as e:
        print(traceback.format_exc())
        return Response({
            "message": "Processing error",
        }, status=500)

    return Response({
        "message": "Processing completed",
    }, status=200)

