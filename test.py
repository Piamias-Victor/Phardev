def process_sales_with_tva_update(pharmacy: Pharmacy, sales_data: dict) -> dict:
    """
    Version DEBUG du traitement des ventes avec logs détaillés
    """
    from data.models import InternalProduct, Sales, InventorySnapshot
    from django.db import transaction
    
    result = {
        'sales_processed': 0,
        'tva_updates': 0,
        'errors': [],
        'debug_info': {
            'total_ventes_received': 0,
            'total_lignes_received': 0,
            'products_not_found': 0,
            'snapshots_not_found': 0,
            'existing_sales_skipped': 0
        }
    }
    
    # Validation de la structure des données
    logger.info("🔍 DEBUG: Analyse de la structure des données")
    
    if not isinstance(sales_data, list):
        logger.error(f"❌ sales_data n'est pas une liste: {type(sales_data)}")
        return result
    
    if len(sales_data) == 0:
        logger.warning("⚠️ sales_data est une liste vide")
        return result
    
    logger.info(f"✅ sales_data est une liste de {len(sales_data)} éléments")
    
    # Analyser le premier élément
    first_element = sales_data[0]
    logger.info(f"🔍 Premier élément clés: {list(first_element.keys())}")
    
    if 'ventes' not in first_element:
        logger.error("❌ Pas de clé 'ventes' dans les données")
        return result
    
    pharmacy_sales = first_element['ventes']
    result['debug_info']['total_ventes_received'] = len(pharmacy_sales)
    
    # Compter le total de lignes
    total_lignes = sum(len(vente.get('lignes', [])) for vente in pharmacy_sales)
    result['debug_info']['total_lignes_received'] = total_lignes
    
    logger.info(f"📊 DEBUG: {len(pharmacy_sales)} ventes reçues avec {total_lignes} lignes")
    
    # Analyser quelques ventes pour comprendre la structure
    if len(pharmacy_sales) > 0:
        sample_vente = pharmacy_sales[0]
        logger.info(f"🔍 Structure vente exemple: {list(sample_vente.keys())}")
        
        if 'lignes' in sample_vente and len(sample_vente['lignes']) > 0:
            sample_ligne = sample_vente['lignes'][0]
            logger.info(f"🔍 Structure ligne exemple: {list(sample_ligne.keys())}")
    
    # Traitement avec logs détaillés
    with transaction.atomic():
        tva_updates_count = 0
        products_not_found = 0
        snapshots_not_found = 0
        existing_sales_skipped = 0
        
        for i, vente in enumerate(pharmacy_sales):
            if i % 1000 == 0:  # Log toutes les 1000 ventes
                logger.info(f"⏳ Traitement vente {i+1}/{len(pharmacy_sales)}")
            
            try:
                # Extraire la date
                date_facture = vente.get('dateFacture', '')
                if not date_facture:
                    continue
                    
                vente_date = date_facture.split('T')[0]  # Format YYYY-MM-DD
                
                # Traiter chaque ligne
                lignes = vente.get('lignes', [])
                for ligne in lignes:
                    try:
                        # Extraire les données
                        prod_id = ligne.get('prodId')
                        code13_ref = ligne.get('code13Ref')
                        qte = ligne.get('qte', 0)
                        tva = ligne.get('tva')
                        
                        if not prod_id or not code13_ref:
                            continue
                        
                        # Recherche du produit interne
                        try:
                            internal_product = InternalProduct.objects.get(
                                pharmacy=pharmacy,
                                code_13_ref_id=code13_ref
                            )
                        except InternalProduct.DoesNotExist:
                            products_not_found += 1
                            if products_not_found <= 5:  # Log seulement les 5 premiers
                                logger.warning(f"⚠️ Produit non trouvé: {code13_ref}")
                            continue
                        
                        # Recherche du snapshot
                        try:
                            latest_snapshot = InventorySnapshot.objects.filter(
                                product=internal_product
                            ).order_by('-date', '-created_at').first()
                            
                            if not latest_snapshot:
                                snapshots_not_found += 1
                                if snapshots_not_found <= 5:
                                    logger.warning(f"⚠️ Pas de snapshot pour: {code13_ref}")
                                continue
                                
                        except Exception as e:
                            logger.error(f"❌ Erreur recherche snapshot {code13_ref}: {e}")
                            continue
                        
                        # Vérifier si la vente existe déjà
                        if Sales.objects.filter(
                            date=vente_date,
                            product_id=latest_snapshot.id,
                            quantity=qte
                        ).exists():
                            existing_sales_skipped += 1
                            continue
                        
                        # Créer la vente
                        Sales.objects.create(
                            date=vente_date,
                            product_id=latest_snapshot.id,
                            quantity=qte
                        )
                        result['sales_processed'] += 1
                        
                        # Mise à jour TVA
                        if tva is not None and internal_product.TVA != tva:
                            internal_product.TVA = tva
                            internal_product.save(update_fields=['TVA'])
                            tva_updates_count += 1
                            
                            if tva_updates_count <= 10:  # Log les 10 premières
                                logger.info(f"🔄 TVA mise à jour {code13_ref}: {tva}%")
                        
                    except Exception as e:
                        error_msg = f"Erreur ligne {prod_id}: {e}"
                        if len(result['errors']) < 10:  # Limite les erreurs loggées
                            logger.error(f"❌ {error_msg}")
                            result['errors'].append(error_msg)
                        continue
                        
            except Exception as e:
                error_msg = f"Erreur vente {vente.get('id', 'unknown')}: {e}"
                if len(result['errors']) < 10:
                    logger.error(f"❌ {error_msg}")
                    result['errors'].append(error_msg)
                continue
        
        # Mettre à jour les stats debug
        result['tva_updates'] = tva_updates_count
        result['debug_info']['products_not_found'] = products_not_found
        result['debug_info']['snapshots_not_found'] = snapshots_not_found
        result['debug_info']['existing_sales_skipped'] = existing_sales_skipped
        
        # Log final détaillé
        logger.info("📊 RÉSULTAT FINAL TRAITEMENT VENTES:")
        logger.info(f"   📥 Ventes reçues: {result['debug_info']['total_ventes_received']}")
        logger.info(f"   📋 Lignes reçues: {result['debug_info']['total_lignes_received']}")
        logger.info(f"   ✅ Ventes créées: {result['sales_processed']}")
        logger.info(f"   🔄 TVA mises à jour: {result['tva_updates']}")
        logger.info(f"   ❌ Produits non trouvés: {result['debug_info']['products_not_found']}")
        logger.info(f"   📦 Snapshots manquants: {result['debug_info']['snapshots_not_found']}")
        logger.info(f"   🔄 Ventes existantes ignorées: {result['debug_info']['existing_sales_skipped']}")
        
        if result['debug_info']['products_not_found'] > 0:
            logger.warning(f"⚠️ PROBLÈME MAJEUR: {result['debug_info']['products_not_found']} produits non trouvés")
            logger.warning("   Vérifiez que les produits ont été importés avant les ventes")
        
        if result['debug_info']['snapshots_not_found'] > 0:
            logger.warning(f"⚠️ PROBLÈME: {result['debug_info']['snapshots_not_found']} snapshots manquants")
            logger.warning("   Les produits existent mais n'ont pas de snapshots d'inventaire")
        
    return result