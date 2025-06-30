from django.conf import settings
from django.conf.urls.static import static
from django.urls import path

from data import views

urlpatterns = [
    # ============================================================================
    # ENDPOINTS EXISTANTS (inchangés)
    # ============================================================================
    
    # WinPharma ancienne API
    path('winpharma/create/products', views.winpharma_create_product, name='winpharma_create_product'),
    path('winpharma/create/orders', views.winpharma_create_order, name='winpharma_create_order'),
    path('winpharma/create/sales', views.winpharma_create_sales, name='winpharma_create_sales'),

    # Dexter
    path('dexter/create/stock', views.dexter_create_stock, name='dexter_create_stock'),
    path('dexter/create/achat', views.dexter_create_achat, name='dexter_create_achat'),
    path('dexter/create/vente', views.dexter_create_vente, name='dexter_create_vente'),

    # WinPharma 2
    path('winpharma_2/create/products', views.winpharma_2_create_product, name='winpharma_2_create_product'),
    path('winpharma_2/create/orders', views.winpharma_2_create_order, name='winpharma_2_create_order'),
    path('winpharma_2/create/sales', views.winpharma_2_create_sales, name='winpharma_2_create_sales'),

    # ============================================================================
    # ENDPOINTS PRODUCTION NOUVELLE API
    # ============================================================================
    
    # Nouvelle API WinPharma (production)
    path('winpharma_new_api/create/products', views.winpharma_new_api_create_product, name='winpharma_new_api_create_product'),
    path('winpharma_new_api/create/orders', views.winpharma_new_api_create_order, name='winpharma_new_api_create_order'),
    path('winpharma_new_api/create/sales', views.winpharma_new_api_create_sales, name='winpharma_new_api_create_sales'),

    # ============================================================================
    # NOUVEAUX ENDPOINTS POUR TESTS NOUVELLE API
    # ============================================================================
    
    # Tests pour nouvelle API WinPharma
    path('test/new_api/products', views.test_new_api_products, name='test_new_api_products'),
    path('test/new_api/orders', views.test_new_api_orders, name='test_new_api_orders'),
    path('test/new_api/sales', views.test_new_api_sales, name='test_new_api_sales'),
    path('test/new_api/summary', views.test_new_api_summary, name='test_new_api_summary'),


path('winpharma_historical/create/products', views.winpharma_historical_create_product, name='winpharma_historical_create_product'),
path('winpharma_historical/create/orders', views.winpharma_historical_create_order, name='winpharma_historical_create_order'),
path('winpharma_historical/create/sales', views.winpharma_historical_create_sales, name='winpharma_historical_create_sales'),

] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)