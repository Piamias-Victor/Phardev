#!/usr/bin/env python3
"""
Analyse comparative des pharmacies 832002810 vs 692020472
pour identifier pourquoi les achats n'apparaissent pas dans le dashboard
"""

import os
import sys
import django
from django.db import connection
import json
from datetime import datetime, timedelta

# Configuration Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'Phardev.settings')
django.setup()

from data.models import Pharmacy, InternalProduct, Order, ProductOrder, Supplier, InventorySnapshot, Sales

def print_separator(title, char="=", width=70):
    """Affiche un séparateur avec titre"""
    print(f"\n{char * width}")
    print(f"  {title}")
    print(f"{char * width}")

def analyze_pharmacy_data():
    """Analyse comparative des deux pharmacies"""
    
    pharmacy_ids = {
        "problematic": "832002810",  # Pharmacie avec problème
        "working": "692020472"       # Pharmacie qui fonctionne
    }
    
    print_separator("ANALYSE COMPARATIVE DES PHARMACIES")
    
    results = {}
    
    for label, id_nat in pharmacy_ids.items():
        try:
            pharmacy = Pharmacy.objects.get(id_nat=id_nat)
            
            # Compter toutes les données
            orders_count = Order.objects.filter(pharmacy=pharmacy).count()
            products_count = InternalProduct.objects.filter(pharmacy=pharmacy).count()
            suppliers_count = Supplier.objects.filter(pharmacy=pharmacy).count()
            
            # Données liées aux inventaires et ventes
            snapshots_count = InventorySnapshot.objects.filter(product__pharmacy=pharmacy).count()
            sales_count = Sales.objects.filter(product__product__pharmacy=pharmacy).count()
            
            # Lignes de commandes
            product_orders_count = ProductOrder.objects.filter(order__pharmacy=pharmacy).count()
            
            # Données récentes (dernières 24h)
            recent_orders = Order.objects.filter(
                pharmacy=pharmacy,
                created_at__gte=datetime.now() - timedelta(hours=24)
            ).count()
            
            recent_product_orders = ProductOrder.objects.filter(
                order__pharmacy=pharmacy,
                created_at__gte=datetime.now() - timedelta(hours=24)
            ).count()
            
            results[label] = {
                'pharmacy': {
                    'id_nat': id_nat,
                    'name': pharmacy.name,
                    'uuid': str(pharmacy.id)
                },
                'counts': {
                    'orders': orders_count,
                    'products': products_count,
                    'suppliers': suppliers_count,
                    'snapshots': snapshots_count,
                    'sales': sales_count,
                    'product_orders': product_orders_count
                },
                'recent_24h': {
                    'orders': recent_orders,
                    'product_orders': recent_product_orders
                }
            }
            
            print(f"\n📊 {label.upper()} - {id_nat} ({pharmacy.name})")
            print(f"   UUID: {pharmacy.id}")
            print(f"   Commandes: {orders_count}")
            print(f"   Produits: {products_count}")
            print(f"   Fournisseurs: {suppliers_count}")
            print(f"   Lignes commandes: {product_orders_count}")
            print(f"   Snapshots: {snapshots_count}")
            print(f"   Ventes: {sales_count}")
            print(f"   Récent (24h): {recent_orders} commandes, {recent_product_orders} lignes")
            
        except Pharmacy.DoesNotExist:
            print(f"❌ Pharmacie {id_nat} non trouvée")
            continue
    
    return results

def analyze_recent_orders():
    """Analyse détaillée des commandes récentes"""
    
    print_separator("ANALYSE DES COMMANDES RÉCENTES")
    
    pharmacy_ids = ["832002810", "692020472"]
    
    for id_nat in pharmacy_ids:
        try:
            pharmacy = Pharmacy.objects.get(id_nat=id_nat)
            
            print(f"\n🔍 PHARMACIE {id_nat} - Dernières 10 commandes:")
            
            recent_orders = Order.objects.filter(pharmacy=pharmacy).order_by('-created_at')[:10]
            
            if not recent_orders:
                print("   ❌ Aucune commande trouvée")
                continue
            
            for i, order in enumerate(recent_orders, 1):
                lines_count = ProductOrder.objects.filter(order=order).count()
                supplier_name = order.supplier.name if order.supplier else "Sans fournisseur"
                
                print(f"   {i:2d}. ID: {order.internal_id:>10} | "
                      f"Fournisseur: {supplier_name[:20]:20} | "
                      f"Lignes: {lines_count:3d} | "
                      f"Créé: {order.created_at.strftime('%Y-%m-%d %H:%M')}")
        
        except Pharmacy.DoesNotExist:
            print(f"❌ Pharmacie {id_nat} non trouvée")

def analyze_suppliers():
    """Analyse des fournisseurs"""
    
    print_separator("ANALYSE DES FOURNISSEURS")
    
    pharmacy_ids = ["832002810", "692020472"]
    
    for id_nat in pharmacy_ids:
        try:
            pharmacy = Pharmacy.objects.get(id_nat=id_nat)
            
            print(f"\n🏢 PHARMACIE {id_nat} - Fournisseurs:")
            
            suppliers = Supplier.objects.filter(pharmacy=pharmacy).order_by('-created_at')[:10]
            
            if not suppliers:
                print("   ❌ Aucun fournisseur trouvé")
                continue
            
            for supplier in suppliers:
                orders_count = Order.objects.filter(supplier=supplier).count()
                print(f"   • {supplier.name[:30]:30} | "
                      f"Code: {supplier.code_supplier:15} | "
                      f"Commandes: {orders_count:3d} | "
                      f"Créé: {supplier.created_at.strftime('%Y-%m-%d')}")
        
        except Pharmacy.DoesNotExist:
            print(f"❌ Pharmacie {id_nat} non trouvée")

def check_data_integrity():
    """Vérification de l'intégrité des données"""
    
    print_separator("VÉRIFICATION DE L'INTÉGRITÉ DES DONNÉES")
    
    # 1. Commandes sans fournisseur
    print("\n🔍 Commandes sans fournisseur:")
    orders_without_supplier = Order.objects.filter(
        supplier__isnull=True,
        pharmacy__id_nat__in=["832002810", "692020472"]
    ).select_related('pharmacy')
    
    for order in orders_without_supplier[:10]:
        print(f"   • Pharmacie {order.pharmacy.id_nat}: Commande {order.internal_id}")
    
    print(f"   Total: {orders_without_supplier.count()}")
    
    # 2. Lignes de commandes orphelines
    print("\n🔍 Lignes de commandes orphelines:")
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(*) 
            FROM data_productorder po 
            WHERE NOT EXISTS (
                SELECT 1 FROM data_order o WHERE o.id = po.order_id
            )
        """)
        orphan_lines = cursor.fetchone()[0]
        print(f"   Total: {orphan_lines}")
    
    # 3. Produits référencés mais manquants
    print("\n🔍 Produits référencés mais manquants:")
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(*) 
            FROM data_productorder po 
            WHERE NOT EXISTS (
                SELECT 1 FROM data_internalproduct ip WHERE ip.id = po.product_id
            )
        """)
        missing_products = cursor.fetchone()[0]
        print(f"   Total: {missing_products}")

def analyze_dashboard_query_simulation():
    """Simule les requêtes du dashboard pour comprendre le problème"""
    
    print_separator("SIMULATION DES REQUÊTES DASHBOARD")
    
    pharmacy_ids = ["832002810", "692020472"]
    
    for id_nat in pharmacy_ids:
        try:
            pharmacy = Pharmacy.objects.get(id_nat=id_nat)
            
            print(f"\n📊 PHARMACIE {id_nat} - Simulation requêtes dashboard:")
            
            # Requête typique du dashboard : commandes avec leurs lignes
            dashboard_orders = Order.objects.filter(
                pharmacy=pharmacy,
                created_at__gte=datetime.now() - timedelta(days=30)  # Dernier mois
            ).select_related('supplier').prefetch_related('product_orders__product')
            
            total_orders = dashboard_orders.count()
            total_lines = sum(order.product_orders.count() for order in dashboard_orders)
            
            print(f"   📋 Commandes (30 derniers jours): {total_orders}")
            print(f"   📦 Lignes de commandes: {total_lines}")
            
            # Commandes par fournisseur
            suppliers_stats = {}
            for order in dashboard_orders:
                supplier_name = order.supplier.name if order.supplier else "Sans fournisseur"
                if supplier_name not in suppliers_stats:
                    suppliers_stats[supplier_name] = 0
                suppliers_stats[supplier_name] += 1
            
            print(f"   🏢 Répartition par fournisseur:")
            for supplier, count in sorted(suppliers_stats.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"      • {supplier[:25]:25}: {count:3d} commandes")
            
            # Vérifier si les lignes ont des produits valides
            valid_lines = 0
            invalid_lines = 0
            
            for order in dashboard_orders[:10]:  # Échantillon
                for line in order.product_orders.all():
                    if line.product and line.product.pharmacy_id == pharmacy.id:
                        valid_lines += 1
                    else:
                        invalid_lines += 1
            
            print(f"   ✅ Lignes valides (échantillon): {valid_lines}")
            print(f"   ❌ Lignes invalides (échantillon): {invalid_lines}")
        
        except Pharmacy.DoesNotExist:
            print(f"❌ Pharmacie {id_nat} non trouvée")

def compare_data_structures():
    """Compare les structures de données entre les deux pharmacies"""
    
    print_separator("COMPARAISON DES STRUCTURES DE DONNÉES")
    
    try:
        pharmacy_problem = Pharmacy.objects.get(id_nat="832002810")
        pharmacy_working = Pharmacy.objects.get(id_nat="692020472")
        
        # Échantillon de commandes récentes de chaque pharmacie
        print("\n🔬 ANALYSE D'ÉCHANTILLONS:")
        
        for label, pharmacy in [("PROBLÉMATIQUE", pharmacy_problem), ("FONCTIONNELLE", pharmacy_working)]:
            print(f"\n{label} - {pharmacy.id_nat}:")
            
            sample_orders = Order.objects.filter(pharmacy=pharmacy).order_by('-created_at')[:3]
            
            for i, order in enumerate(sample_orders, 1):
                lines = ProductOrder.objects.filter(order=order)[:3]  # 3 premières lignes
                
                print(f"   📋 Commande {i}: ID={order.internal_id}")
                print(f"      • Fournisseur: {order.supplier.name if order.supplier else 'None'}")
                print(f"      • Date: {order.created_at}")
                print(f"      • Lignes: {lines.count()}")
                
                for j, line in enumerate(lines, 1):
                    print(f"         {j}. Produit: {line.product.name[:30]} (ID:{line.product.internal_id})")
                    print(f"            Qté: {line.qte}, Reçue: {line.qte_r}")
    
    except Pharmacy.DoesNotExist as e:
        print(f"❌ Erreur: {e}")

def main():
    """Fonction principale d'analyse"""
    
    print("🔍 ANALYSE COMPARATIVE DES PHARMACIES")
    print("Pharmacie problématique: 832002810")
    print("Pharmacie fonctionnelle: 692020472")
    
    # 1. Analyse générale des données
    results = analyze_pharmacy_data()
    
    # 2. Analyse des commandes récentes
    analyze_recent_orders()
    
    # 3. Analyse des fournisseurs
    analyze_suppliers()
    
    # 4. Vérification de l'intégrité
    check_data_integrity()
    
    # 5. Simulation des requêtes dashboard
    analyze_dashboard_query_simulation()
    
    # 6. Comparaison des structures
    compare_data_structures()
    
    # 7. Résumé et recommandations
    print_separator("RÉSUMÉ ET RECOMMANDATIONS")
    
    if 'problematic' in results and 'working' in results:
        prob = results['problematic']['counts']
        work = results['working']['counts']
        
        print(f"\n📊 COMPARAISON QUANTITATIVE:")
        print(f"   Commandes:       {prob['orders']:6d} vs {work['orders']:6d}")
        print(f"   Lignes commandes: {prob['product_orders']:6d} vs {work['product_orders']:6d}")
        print(f"   Fournisseurs:    {prob['suppliers']:6d} vs {work['suppliers']:6d}")
        print(f"   Produits:        {prob['products']:6d} vs {work['products']:6d}")
        
        # Calculer les ratios
        if prob['orders'] > 0:
            prob_ratio = prob['product_orders'] / prob['orders']
        else:
            prob_ratio = 0
            
        if work['orders'] > 0:
            work_ratio = work['product_orders'] / work['orders']
        else:
            work_ratio = 0
        
        print(f"\n📈 RATIOS:")
        print(f"   Lignes/Commande: {prob_ratio:.2f} vs {work_ratio:.2f}")
        
        print(f"\n💡 RECOMMANDATIONS:")
        if prob['orders'] > 0 and prob['product_orders'] == 0:
            print("   ❗ Problème détecté: Commandes sans lignes")
            print("   🔧 Vérifier le processus de création des ProductOrder")
        elif prob['orders'] == 0:
            print("   ❗ Problème détecté: Aucune commande")
            print("   🔧 Vérifier le processus de création des Order")
        else:
            print("   ✅ Structure de données semble normale")
            print("   🔧 Problème probablement dans l'affichage du dashboard")

if __name__ == "__main__":
    main()