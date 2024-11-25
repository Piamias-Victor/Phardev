import logging
import traceback
import json


from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.http import JsonResponse

from rest_framework import status
from django.utils import timezone
from tqdm import tqdm
from data.models import (Product, Pharmacy, Order,
                         Sales,
                         Supplier, ProductOrder)


@api_view(['POST'])
def winpharma_create_product(request):
    """
    Endpoint for creating or updating products linked to a pharmacy.
    """
    pharmacy, _ = Pharmacy.objects.get_or_create(name=request.headers.get('Pharmacy-id'))

    errors = []
    products_created, products_updated = 0, 0

    for obj in tqdm(request.data):
        try:
            product, created = Product.objects.update_or_create(
                pharmacy=pharmacy,
                internal_id=obj['id'],
                defaults={
                    'code_13_ref': obj.get('code13Ref', ''),
                    'name': obj.get('nom', ''),
                    'stock': obj.get('stock', 0),
                    'TVA': obj.get('TVA', 0.0),
                    'price_with_tax': obj.get('prixTtc', 0.0),
                    'weighted_average_price': obj.get('prixMP', 0.0),
                },
            )
            if created:
                products_created += 1
            else:
                products_updated += 1

        except Exception as e:
            # Logguer l'erreur et collecter l'objet problématique
            print(traceback.format_exc())
            print(obj)
            errors.append({"product": obj, "error": str(e)})

    return Response({
        "message": "Processing completed",
        "products_created": products_created,
        "products_updated": products_updated,
        "errors": errors,
    }, status=200)


@api_view(['POST'])
def winpharma_create_order(request):
    """
    Endpoint for creating or updating orders linked to a pharmacy.
    """
    pharmacy, _ = Pharmacy.objects.get_or_create(name=request.headers.get('Pharmacy-id'))

    errors = []
    orders_created, orders_updated = 0, 0
    product_orders_created, product_orders_updated = 0, 0

    for obj in tqdm(request.data):
        try:
            supplier, _ = Supplier.objects.get_or_create(
                code_supplier=obj['codeFourn'],
                defaults={'name': obj['nomFourn']}
            )

            order, created = Order.objects.update_or_create(
                pharmacy=pharmacy,
                supplier=supplier,
                internal_id=obj['idCmd'],
                defaults={
                    'step': obj['etape'],
                    'sent_date': obj['envoi'],
                    'delivery_date': obj['dateLivraison'],
                }
            )

            if created:
                orders_created += 1
            else:
                orders_updated += 1

            for product_order in obj['produits']:
                product, _ = Product.objects.get_or_create(
                    pharmacy=pharmacy,
                    internal_id=product_order['prodId']
                )

                product_order_obj, created = ProductOrder.objects.update_or_create(
                    product=product,
                    order=order,
                    defaults={
                        'qte': product_order['qte'],
                        'qte_r': product_order['qteR'],
                        'qte_a': product_order['qteA'],
                        'qte_ug': product_order['qteUG'],
                        'qte_ec': product_order['qteEC'],
                        'qte_ar': product_order['qteAReceptionner'],
                    }
                )

                if created:
                    product_orders_created += 1
                else:
                    product_orders_updated += 1

        except Exception as e:
            print(traceback.format_exc())
            print(obj)
            errors.append({"order": obj, "error": str(e)})
    return Response({
        "message": "Processing completed",
        "orders_created": orders_created,
        "orders_updated": orders_updated,
        "product_orders_created": product_orders_created,
        "product_orders_updated": product_orders_updated,
        "errors": errors,
    }, status=200)


@api_view(['POST'])
def winpharma_create_sales(request):
    """
    Endpoint for creating or updating sales records linked to a pharmacy and its products.
    """
    pharmacy, _ = Pharmacy.objects.get_or_create(name=request.headers.get('Pharmacy-id'))

    errors = []
    sales_created, sales_updated = 0, 0

    for obj in tqdm(request.data):
        try:
            # Récupération ou création du produit
            product, _ = Product.objects.get_or_create(
                pharmacy=pharmacy,
                internal_id=obj['prodId']
            )

            # Récupération ou création de la vente
            sale, created = Sales.objects.update_or_create(
                pharmacy=pharmacy,
                product=product,
                time=obj['heure'],
                defaults={
                    'quantity': obj['qte'],
                }
            )

            if created:
                sales_created += 1
            else:
                sales_updated += 1

        except Exception as e:
            print(traceback.format_exc())
            print(obj)
            errors.append({"sale": obj, "error": str(e)})

    # Résumé des opérations
    return Response({
        "message": "Processing completed",
        "sales_created": sales_created,
        "sales_updated": sales_updated,
        "errors": errors,
    }, status=200)


# @api_view(['GET'])
# def trigger_rescrape(request):
#     lambda_allocated = 0
#
#     #TODO rechange magnitude to 7
#     magnitudes = {0: timezone.now() - timezone.timedelta(days=10), 1: timezone.now() - timezone.timedelta(days=10)}
#
#     sources = SourceMapping.objects.filter(rescrape=True).order_by('id')
#     watches = Watch.objects.filter(is_live=True, is_getting_rescrape=False).order_by('last_scrape_time')
#     try:
#         # For each magnitude of rescrape
#         for magnitude, frequency in reversed(magnitudes.items()):
#             current_watches = watches.filter(magnitude=magnitude, last_scrape_time__lt=frequency).values_list('url', 'source_id')
#             if current_watches.count() == 0:
#                 continue
#
#             # For each source calculate the number of lambda to allocate
#             for source in sources:
#                 source_watches = current_watches.filter(source_id=source).values_list('url', flat=True)
#                 count_of_watches = source_watches.count()
#
#                 if count_of_watches == 0:
#                     continue
#                 # Round up the number of lambda to create
#                 number_of_lambda = math.ceil(count_of_watches / CAPACITY_OF_LAMBDA)
#
#                 # Create lambda until we hit the limit
#                 for i in range(0, number_of_lambda):
#
#                     if lambda_allocated < 20:
#                         watches_scrape = source_watches[i * CAPACITY_OF_LAMBDA: (i + 1) * CAPACITY_OF_LAMBDA]
#                         source_watches.filter(url__in=list(watches_scrape)).update(is_getting_rescrape=True)
#                         list_of_urls = json.dumps({'data': list(watches_scrape)})
#
#                         LAMBDA_CLIENT.invoke(FunctionName=source.lambda_name,
#                                              InvocationType='Event',
#                                              LogType='None',
#                                              Payload=list_of_urls)
#                         lambda_allocated += 1
#
#                     else:
#                         return Response(status=status.HTTP_226_IM_USED)
#     except:
#         RSC_LOGGER.error('MAGNITUDE ERROR')
#         RSC_LOGGER.error(traceback.format_exc())
#
#     return Response(status=status.HTTP_200_OK)
