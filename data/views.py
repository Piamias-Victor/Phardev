import traceback

from rest_framework.decorators import api_view
from rest_framework.response import Response

from data.models import (Pharmacy)
from data.services import dexter, winpharma, winpharma_2


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
