#!/usr/bin/env python3
"""
Script de diagnostic pour identifier pourquoi les achats n'apparaissent pas dans le dashboard
"""

import os
import sys
import django
from django.db import transaction, connection
import json
import logging

# Configuration Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'Phardev.settings')
django.setup()

from data.models import Pharmacy, InternalProduct, Order, ProductOrder, Supplier, InventorySnapshot
from data.services.winpharma_historical import process_order

# Configuration du logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def print_separator(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def analyze_database_state(pharmacy_id="062044623"):
    """Analyse l'état actuel de la base de données"""
    print_separator("ANALYSE DE L'ÉTAT DE LA BASE DE DONNÉES")
    
    try:
        pharmacy = Pharmacy.objects.get(id_nat=pharmacy_id)
        print(f"✅ Pharmacie trouvée: {pharmacy.name} (UUID: {pharmacy.id})")
    except Pharmacy.DoesNotExist:
        print(f"❌ Pharmacie {pharmacy_id} non trouvée")
        return None
    
    # Compter les données
    products_count = InternalProduct.objects.filter(pharmacy=pharmacy).count()
    orders_count = Order.objects.filter(pharmacy=pharmacy).count()
    product_orders_count = ProductOrder.objects.filter(order__pharmacy=pharmacy).count()
    suppliers_count = Supplier.objects.filter(pharmacy=pharmacy).count()
    snapshots_count = InventorySnapshot.objects.filter(product__pharmacy=pharmacy).count()
    
    print(f"📊 Données actuelles:")
    print(f"   - Produits internes: {products_count}")
    print(f"   - Commandes: {orders_count}")
    print(f"   - Lignes de commande: {product_orders_count}")
    print(f"   - Fournisseurs: {suppliers_count}")
    print(f"   - Snapshots d'inventaire: {snapshots_count}")
    
    # Dernières données
    if orders_count > 0:
        latest_order = Order.objects.filter(pharmacy=pharmacy).order_by('-created_at').first()
        print(f"   - Dernière commande: {latest_order.internal_id} (créée le {latest_order.created_at})")
        
        # Détails de la dernière commande
        latest_order_lines = ProductOrder.objects.filter(order=latest_order).count()
        print(f"   - Lignes de la dernière commande: {latest_order_lines}")
    
    return pharmacy

def test_with_real_data(pharmacy_id="062044623"):
    """Test avec les vraies données d'achats de l'API"""
    print_separator("TEST AVEC DONNÉES RÉELLES D'ACHATS")
    
    # Données d'exemple basées sur la documentation API
    sample_data = [{
        "cip_pharma": pharmacy_id,
        "achats": [
            {
                "id": 999999,  # ID de test
                "dateLivraison": "2025-07-08T00:00:00",
                "codeFourn": "TEST_FOURNISSEUR",
                "nomFourn": "FOURNISSEUR DE TEST",
                "typeFourn": 1,
                "dateEnvoi": "2025-07-07T00:00:00",
                "channel": "pml",
                "lignes": [
                    {
                        "prodId": 9999999,  # ID de test
                        "code13Ref": "1234567890123",
                        "extraCodes": ["1234567890123"],
                        "nomProduit": "PRODUIT DE TEST",
                        "qteR": 2,
                        "qteC": 2,
                        "qteUG": 0,
                        "qteEC": 0,
                        "totalR": 2,
                        "prix": 10.50,
                        "remiseFinal": 0.00
                    }
                ]
            }
        ],
        "status": "success",
        "status_detail": "Test data"
    }]
    
    try:
        pharmacy = Pharmacy.objects.get(id_nat=pharmacy_id)
        
        print(f"🔍 Test avec données d'achats de test")
        print(f"   - 1 commande avec 1 ligne")
        print(f"   - Fournisseur: TEST_FOURNISSEUR")
        print(f"   - Produit: PRODUIT DE TEST (ID: 9999999)")
        
        # Compter les données avant
        orders_before = Order.objects.filter(pharmacy=pharmacy).count()
        product_orders_before = ProductOrder.objects.filter(order__pharmacy=pharmacy).count()
        suppliers_before = Supplier.objects.filter(pharmacy=pharmacy).count()
        products_before = InternalProduct.objects.filter(pharmacy=pharmacy).count()
        
        print(f"\n📊 Données AVANT traitement:")
        print(f"   - Commandes: {orders_before}")
        print(f"   - Lignes commandes: {product_orders_before}")
        print(f"   - Fournisseurs: {suppliers_before}")
        print(f"   - Produits: {products_before}")
        
        # Traiter les données
        try:
            with transaction.atomic():
                result = process_order(pharmacy, sample_data)
                print(f"\n✅ Traitement terminé sans erreur")
                print(f"📋 Résultat: {result}")
        except Exception as e:
            print(f"\n❌ Erreur lors du traitement: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        # Compter les données après
        orders_after = Order.objects.filter(pharmacy=pharmacy).count()
        product_orders_after = ProductOrder.objects.filter(order__pharmacy=pharmacy).count()
        suppliers_after = Supplier.objects.filter(pharmacy=pharmacy).count()
        products_after = InternalProduct.objects.filter(pharmacy=pharmacy).count()
        
        print(f"\n📊 Données APRÈS traitement:")
        print(f"   - Commandes: {orders_after} (+{orders_after - orders_before})")
        print(f"   - Lignes commandes: {product_orders_after} (+{product_orders_after - product_orders_before})")
        print(f"   - Fournisseurs: {suppliers_after} (+{suppliers_after - suppliers_before})")
        print(f"   - Produits: {products_after} (+{products_after - products_before})")
        
        # Vérifier les nouvelles données
        if orders_after > orders_before:
            test_order = Order.objects.filter(pharmacy=pharmacy, internal_id=999999).first()
            if test_order:
                print(f"\n✅ Commande de test créée:")
                print(f"   - ID interne: {test_order.internal_id}")
                print(f"   - Fournisseur: {test_order.supplier.name if test_order.supplier else 'Aucun'}")
                print(f"   - Date envoi: {test_order.sent_date}")
                print(f"   - Date livraison: {test_order.delivery_date}")
                
                # Vérifier les lignes
                test_lines = ProductOrder.objects.filter(order=test_order)
                print(f"   - Lignes: {test_lines.count()}")
                for line in test_lines:
                    print(f"     * Produit: {line.product.name} (ID: {line.product.internal_id})")
                    print(f"     * Qté commandée: {line.qte}")
                    print(f"     * Qté reçue: {line.qte_r}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
        return False

def analyze_existing_orders(pharmacy_id="062044623"):
    """Analyse les commandes existantes"""
    print_separator("ANALYSE DES COMMANDES EXISTANTES")
    
    try:
        pharmacy = Pharmacy.objects.get(id_nat=pharmacy_id)
        
        # Toutes les commandes
        orders = Order.objects.filter(pharmacy=pharmacy).order_by('-created_at')[:10]
        
        print(f"📋 Dernières 10 commandes:")
        for order in orders:
            lines_count = ProductOrder.objects.filter(order=order).count()
            supplier_name = order.supplier.name if order.supplier else "Sans fournisseur"
            print(f"   - ID: {order.internal_id} | Fournisseur: {supplier_name} | Lignes: {lines_count} | Créé: {order.created_at}")
        
        # Fournisseurs
        suppliers = Supplier.objects.filter(pharmacy=pharmacy)
        print(f"\n🏢 Fournisseurs ({suppliers.count()}):")
        for supplier in suppliers[:10]:
            orders_count = Order.objects.filter(supplier=supplier).count()
            print(f"   - {supplier.name} (Code: {supplier.code_supplier}) | Commandes: {orders_count}")
        
        # Produits avec commandes
        products_with_orders = InternalProduct.objects.filter(
            pharmacy=pharmacy,
            product_orders__isnull=False
        ).distinct()[:10]
        
        print(f"\n📦 Produits avec commandes ({products_with_orders.count()}):")
        for product in products_with_orders:
            orders_count = ProductOrder.objects.filter(product=product).count()
            print(f"   - {product.name} (ID: {product.internal_id}) | Commandes: {orders_count}")
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback
        traceback.print_exc()

def check_database_constraints():
    """Vérifie les contraintes de base de données"""
    print_separator("VÉRIFICATION DES CONTRAINTES DE BASE")
    
    # Vérifier les index et contraintes
    with connection.cursor() as cursor:
        # Contraintes des tables importantes
        tables_to_check = [
            'data_order',
            'data_productorder', 
            'data_supplier',
            'data_internalproduct'
        ]
        
        for table in tables_to_check:
            cursor.execute(f"""
                SELECT constraint_name, constraint_type 
                FROM information_schema.table_constraints 
                WHERE table_name = '{table}' 
                AND table_schema = 'public'
            """)
            constraints = cursor.fetchall()
            
            print(f"📋 Table {table}:")
            for constraint_name, constraint_type in constraints:
                print(f"   - {constraint_name}: {constraint_type}")

def main():
    """Fonction principale de diagnostic"""
    print_separator("🔍 DIAGNOSTIC DES PROBLÈMES D'ACHATS/ORDERS")
    
    pharmacy_id = input("ID de la pharmacie à analyser (défaut: 062044623): ").strip()
    if not pharmacy_id:
        pharmacy_id = "062044623"
    
    # 1. Analyser l'état actuel de la base
    pharmacy = analyze_database_state(pharmacy_id)
    
    if not pharmacy:
        print("❌ Impossible de continuer sans pharmacie valide")
        return
    
    # 2. Analyser les commandes existantes
    analyze_existing_orders(pharmacy_id)
    
    # 3. Vérifier les contraintes
    check_database_constraints()
    
    # 4. Test avec données réelles
    print(f"\n🧪 Voulez-vous tester avec des données d'achats de test ?")
    test_choice = input("(o/n): ").lower().strip()
    
    if test_choice == 'o':
        success = test_with_real_data(pharmacy_id)
        if success:
            print(f"\n✅ Test réussi - les données sont bien traitées")
            print(f"🔍 Si les données n'apparaissent pas dans votre dashboard,")
            print(f"   le problème vient de l'affichage/requêtes du dashboard")
        else:
            print(f"\n❌ Test échoué - problème dans le traitement des données")
    
    print_separator("FIN DU DIAGNOSTIC")

if __name__ == "__main__":
    main()