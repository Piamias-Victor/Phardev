#!/usr/bin/env python3
"""
Script pour nettoyer toutes les données de test de la pharmacie 832002810
"""

import psycopg2

def clean_pharmacy_data(pharmacy_id_nat):
    """Supprime toutes les données d'une pharmacie"""
    try:
        print(f"🧹 NETTOYAGE DES DONNÉES DE TEST")
        print(f"🏥 Pharmacie ID: {pharmacy_id_nat}")
        print(f"=" * 50)
        
        # Connection DB
        conn = psycopg2.connect(
            host="phardev.cts8s2sgms8l.eu-west-3.rds.amazonaws.com",
            database="postgres",
            user="postgres",
            password="NNPwUUstdTonFYZwfisO",
            port=5432
        )
        
        cursor = conn.cursor()
        
        # 1. Trouver l'UUID de la pharmacie
        cursor.execute("SELECT id FROM data_pharmacy WHERE id_nat = %s", (pharmacy_id_nat,))
        result = cursor.fetchone()
        
        if not result:
            print(f"❌ Pharmacie {pharmacy_id_nat} introuvable")
            return False
            
        pharmacy_uuid = result[0]
        print(f"📋 Pharmacie UUID: {pharmacy_uuid}")
        
        # 2. Compter avant suppression
        print(f"\n📊 DONNÉES AVANT SUPPRESSION:")
        
        # Compter sales
        cursor.execute("""
            SELECT COUNT(*) FROM data_sales s
            JOIN data_inventorysnapshot inv ON s.product_id = inv.id
            JOIN data_internalproduct ip ON inv.product_id = ip.id  
            WHERE ip.pharmacy_id = %s
        """, (pharmacy_uuid,))
        sales_count = cursor.fetchone()[0]
        print(f"   💰 Ventes: {sales_count}")
        
        # Compter orders
        cursor.execute("SELECT COUNT(*) FROM data_order WHERE pharmacy_id = %s", (pharmacy_uuid,))
        orders_count = cursor.fetchone()[0]
        print(f"   📦 Commandes: {orders_count}")
        
        # Compter product orders
        cursor.execute("""
            SELECT COUNT(*) FROM data_productorder po
            JOIN data_order o ON po.order_id = o.id
            WHERE o.pharmacy_id = %s
        """, (pharmacy_uuid,))
        product_orders_count = cursor.fetchone()[0]
        print(f"   📋 Lignes commandes: {product_orders_count}")
        
        # Compter snapshots
        cursor.execute("""
            SELECT COUNT(*) FROM data_inventorysnapshot inv
            JOIN data_internalproduct ip ON inv.product_id = ip.id  
            WHERE ip.pharmacy_id = %s
        """, (pharmacy_uuid,))
        snapshots_count = cursor.fetchone()[0]
        print(f"   📸 Snapshots: {snapshots_count}")
        
        # Compter products
        cursor.execute("SELECT COUNT(*) FROM data_internalproduct WHERE pharmacy_id = %s", (pharmacy_uuid,))
        products_count = cursor.fetchone()[0]
        print(f"   🏷️ Produits: {products_count}")
        
        # Compter suppliers
        cursor.execute("SELECT COUNT(*) FROM data_supplier WHERE pharmacy_id = %s", (pharmacy_uuid,))
        suppliers_count = cursor.fetchone()[0]
        print(f"   🏢 Fournisseurs: {suppliers_count}")
        
        # 3. Supprimer dans l'ordre des dépendances
        print(f"\n🗑️ SUPPRESSION EN COURS...")
        
        # Sales (dépend de snapshots)
        cursor.execute("""
            DELETE FROM data_sales 
            WHERE product_id IN (
                SELECT inv.id FROM data_inventorysnapshot inv
                JOIN data_internalproduct ip ON inv.product_id = ip.id  
                WHERE ip.pharmacy_id = %s
            )
        """, (pharmacy_uuid,))
        print(f"   ✅ Sales supprimées: {cursor.rowcount}")
        
        # ProductOrder (dépend de orders et products)
        cursor.execute("""
            DELETE FROM data_productorder 
            WHERE order_id IN (
                SELECT id FROM data_order WHERE pharmacy_id = %s
            )
        """, (pharmacy_uuid,))
        print(f"   ✅ ProductOrders supprimées: {cursor.rowcount}")
        
        # Orders
        cursor.execute("DELETE FROM data_order WHERE pharmacy_id = %s", (pharmacy_uuid,))
        print(f"   ✅ Orders supprimées: {cursor.rowcount}")
        
        # Inventory snapshots
        cursor.execute("""
            DELETE FROM data_inventorysnapshot 
            WHERE product_id IN (
                SELECT id FROM data_internalproduct WHERE pharmacy_id = %s
            )
        """, (pharmacy_uuid,))
        print(f"   ✅ Snapshots supprimés: {cursor.rowcount}")
        
        # Internal products
        cursor.execute("DELETE FROM data_internalproduct WHERE pharmacy_id = %s", (pharmacy_uuid,))
        print(f"   ✅ Products supprimés: {cursor.rowcount}")
        
        # Suppliers
        cursor.execute("DELETE FROM data_supplier WHERE pharmacy_id = %s", (pharmacy_uuid,))
        print(f"   ✅ Suppliers supprimés: {cursor.rowcount}")
        
        # Pharmacy (en dernier)
        cursor.execute("DELETE FROM data_pharmacy WHERE id = %s", (pharmacy_uuid,))
        print(f"   ✅ Pharmacy supprimée: {cursor.rowcount}")
        
        # 4. Commit les changements
        conn.commit()
        
        print(f"\n🎉 NETTOYAGE TERMINÉ !")
        print(f"✅ Toutes les données de la pharmacie {pharmacy_id_nat} ont été supprimées")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur lors du nettoyage: {e}")
        return False

def main():
    """Fonction principale"""
    pharmacy_id = "832002810"
    
    print(f"⚠️  ATTENTION: Ce script va supprimer TOUTES les données de test !")
    print(f"🏥 Pharmacie: {pharmacy_id}")
    
    confirm = input("\n❓ Confirmes-tu la suppression ? (oui/non): ").lower().strip()
    
    if confirm in ['oui', 'o', 'yes', 'y']:
        success = clean_pharmacy_data(pharmacy_id)
        if success:
            print(f"\n🚀 PRÊT POUR UN NOUVEAU TEST PROPRE !")
            print(f"📝 Prochaine étape: Lancer la Lambda pour le 20 juin uniquement")
        else:
            print(f"\n❌ Nettoyage échoué")
    else:
        print(f"\n⏹️ Nettoyage annulé")

if __name__ == "__main__":
    main()