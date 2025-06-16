from django.conf import settings
from django.conf.urls.static import static
from django.urls import path

from data import views

urlpatterns = [
                  path('winpharma/create/products', views.winpharma_create_product, name='winpharma_create_product'),
                  path('winpharma/create/orders', views.winpharma_create_order, name='winpharma_create_order'),
                  path('winpharma/create/sales', views.winpharma_create_sales, name='winpharma_create_sales'),

                  path('dexter/create/stock', views.dexter_create_stock, name='dexter_create_stock'),
                  path('dexter/create/achat', views.dexter_create_achat, name='dexter_create_achat'),
                  path('dexter/create/vente', views.dexter_create_vente, name='dexter_create_vente'),

                  path('winpharma_2/create/products', views.winpharma_2_create_product, name='winpharma_2_create_product'),
                  path('winpharma_2/create/orders', views.winpharma_2_create_order, name='winpharma_2_create_order'),
                  path('winpharma_2/create/sales', views.winpharma_2_create_sales, name='winpharma_2_create_sales'),
              ] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
