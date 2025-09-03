#!/usr/bin/env python3
"""
Script SAFE pour supprimer UNIQUEMENT la pharmacie Mouysset V2
"""

import django
import os
import sys
import logging
from django.db import transaction

# Configuration Django
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'phardev.settings')
django.setup()

from data.models import Pharmacy, Product, Snapshot, Order, Sale

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def delete_mouysset_pharmacy():
    """Supprime UNIQUEMENT la pharmacie Mouysset V2"""
    
    PHARMACY_NAME = "Pharmacie Mouysset V2"
    PHARMACY_ID_NAT = "832002810"
    
    logger.info("=" * 60)
    logger.info("🎯 SUPPRESSION PHARMACIE MOUYSSET V2 UNIQUEMENT")
    logger.info("=" * 60)
    
    # Chercher la pharmacie par nom OU id_nat
    pharmacy = None
    
    try:
        pharmacy = Pharmacy.objects.get(name=PHARMACY_NAME)
        logger.info(f"✅ Pharmacie trouvée par nom: {pharmacy.name}")
    except Pharmacy.DoesNotExist:
        try:
            pharmacy = Pharmacy.objects.get(id_nat=PHARMACY_ID_NAT)
            logger.info(f"✅ Pharmacie trouvée par id_nat: {pharmacy.id_nat}")
        except Pharmacy.DoesNotExist:
            logger.info("ℹ️ Pharmacie Mouysset V2 non trouvée - rien à supprimer")
            return True
    
    # Afficher les détails de la pharmacie trouvée
    logger.info(f"🏥 Pharmacie à supprimer:")
    logger.info(f"   Nom: {pharmacy.name}")
    logger.info(f"   ID National: {pharmacy.id_nat}")
    logger.info(f"   ID: {pharmacy.id}")
    
    # Compter les données associées
    try:
        snapshot_count = Snapshot.objects.filter(pharmacy=pharmacy).count()
        order_count = Order.objects.filter(pharmacy=pharmacy).count()
        sale_count = Sale.objects.filter(pharmacy=pharmacy).count()
        product_count = Product.objects.filter(pharmacy=pharmacy).count()
        
        logger.info(f"📊 Données associées à supprimer:")
        logger.info(f"   Products: {product_count}")
        logger.info(f"   Snapshots: {snapshot_count}")
        logger.info(f"   Orders: {order_count}")
        logger.info(f"   Sales: {sale_count}")
        
        total_records = product_count + snapshot_count + order_count + sale_count + 1
        logger.info(f"   TOTAL: {total_records} enregistrements")
        
        # Demander confirmation
        print(f"\n⚠️ CONFIRMATION REQUISE")
        print(f"Vous allez supprimer {total_records} enregistrements pour la pharmacie '{pharmacy.name}'")
        confirm = input("Tapez 'SUPPRIMER MOUYSSET' pour confirmer: ")
        
        if confirm != "SUPPRIMER MOUYSSET":
            logger.info("❌ Suppression annulée")
            return False
        
        # Suppression avec transaction
        logger.info("🗑️ Début de la suppression...")
        
        with transaction.atomic():
            # Supprimer dans l'ordre des dépendances
            deleted_snapshots = Snapshot.objects.filter(pharmacy=pharmacy).delete()
            logger.info(f"✅ Snapshots supprimés: {deleted_snapshots[0]}")
            
            deleted_orders = Order.objects.filter(pharmacy=pharmacy).delete()
            logger.info(f"✅ Orders supprimés: {deleted_orders[0]}")
            
            deleted_sales = Sale.objects.filter(pharmacy=pharmacy).delete()
            logger.info(f"✅ Sales supprimés: {deleted_sales[0]}")
            
            deleted_products = Product.objects.filter(pharmacy=pharmacy).delete()
            logger.info(f"✅ Products supprimés: {deleted_products[0]}")
            
            # Supprimer la pharmacie
            pharmacy_name = pharmacy.name
            pharmacy.delete()
            logger.info(f"✅ Pharmacie '{pharmacy_name}' supprimée")
        
        logger.info("🎉 Suppression terminée avec succès!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la suppression: {e}")
        return False

def check_other_pharmacies():
    """Affiche les autres pharmacies pour vérifier qu'on ne touche qu'à Mouysset"""
    
    logger.info("\n🔍 Vérification des autres pharmacies:")
    
    other_pharmacies = Pharmacy.objects.exclude(
        name="Pharmacie Mouysset V2"
    ).exclude(
        id_nat="832002810"
    )
    
    if other_pharmacies.exists():
        logger.info(f"✅ {other_pharmacies.count()} autres pharmacies trouvées (NON touchées):")
        for p in other_pharmacies[:5]:  # Afficher max 5
            logger.info(f"   - {p.name} ({p.id_nat})")
        if other_pharmacies.count() > 5:
            logger.info(f"   ... et {other_pharmacies.count() - 5} autres")
    else:
        logger.info("ℹ️ Aucune autre pharmacie dans la base")

def main():
    logger.info("🧹 SCRIPT DE SUPPRESSION PHARMACIE MOUYSSET V2 SEULEMENT")
    
    # Vérifier les autres pharmacies d'abord
    check_other_pharmacies()
    
    # Supprimer uniquement Mouysset
    success = delete_mouysset_pharmacy()
    
    if success:
        logger.info("\n✅ SCRIPT TERMINÉ - Pharmacie Mouysset V2 supprimée")
    else:
        logger.error("\n❌ SCRIPT TERMINÉ AVEC ERREUR")

if __name__ == "__main__":
    main()