#!/usr/bin/env python3
"""
Script pour vérifier un problème spécifique dans process_order
Ce script simule le traitement d'un achat problématique pour identifier l'erreur
"""

import json
import traceback
import sys

# Charger l'achat problématique
try:
    with open('achat_problematique_185200.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    print("Données chargées avec succès")
except Exception as e:
    print(f"Erreur lors du chargement du fichier: {e}")
    sys.exit(1)

# Fonction de simulation simplifiée pour tester le traitement
def simulate_process_order(data):
    """Simule le traitement des données pour détecter des problèmes potentiels"""
    print("\nSimulation du traitement:")
    
    # 1. Analyser les données
    if not data or not isinstance(data, list) or len(data) == 0:
        print("❌ Données vides ou format incorrect")
        return
    
    # 2. Vérifier la structure de premier niveau
    if 'cip_pharma' not in data[0] or 'achats' not in data[0]:
        print(f"❌ Structure incorrecte: {list(data[0].keys())}")
        return
    
    # 3. Analyser les achats
    cip_pharma = data[0]['cip_pharma']
    achats = data[0]['achats']
    print(f"Pharmacy ID: {cip_pharma}")
    print(f"Nombre d'achats: {len(achats)}")
    
    # 4. Simuler le traitement de chaque achat
    for achat in achats:
        try:
            # Vérifier les champs obligatoires
            if 'id' not in achat:
                print(f"❌ Champ 'id' manquant dans l'achat")
                continue
                
            # Tester la conversion en int
            try:
                order_id = int(achat['id'])
                print(f"✅ ID converti en int: {order_id}")
            except (ValueError, TypeError):
                print(f"❌ Erreur de conversion de l'ID: {achat['id']}")
                continue
                
            # Vérifier le fournisseur
            if 'codeFourn' not in achat:
                print(f"❌ Champ 'codeFourn' manquant dans l'achat {order_id}")
                continue
                
            supplier_id = achat['codeFourn']
            supplier_name = achat.get('nomFourn', supplier_id)
            print(f"✅ Fournisseur: {supplier_id} ({supplier_name})")
            
            # Vérifier les lignes
            if 'lignes' not in achat:
                print(f"❌ Champ 'lignes' manquant dans l'achat {order_id}")
                continue
                
            lignes = achat['lignes']
            print(f"Nombre de lignes: {len(lignes)}")
            
            # Simuler le traitement des lignes
            for i, ligne in enumerate(lignes):
                try:
                    # Vérifier prodId
                    if 'prodId' not in ligne:
                        print(f"❌ Champ 'prodId' manquant dans la ligne {i+1}")
                        continue
                        
                    # Tester la conversion en int
                    try:
                        prod_id = int(ligne['prodId'])
                        print(f"✅ prodId {i+1} converti en int: {prod_id}")
                    except (ValueError, TypeError):
                        print(f"❌ Erreur de conversion du prodId: {ligne['prodId']}")
                        continue
                    
                    # Vérifier les autres champs
                    print(f"Vérification des quantités de la ligne {i+1}:")
                    for field in ['qteC', 'qteR', 'qteUG', 'qteEC']:
                        if field in ligne:
                            try:
                                value = int(ligne[field])
                                print(f"  ✅ {field}: {value}")
                            except (ValueError, TypeError):
                                print(f"  ❌ Erreur de conversion de {field}: {ligne[field]}")
                        else:
                            print(f"  ⚠️ Champ {field} manquant")
                
                except Exception as e:
                    print(f"❌ Erreur lors du traitement de la ligne {i+1}: {e}")
                    traceback.print_exc()
        
        except Exception as e:
            print(f"❌ Erreur lors du traitement de l'achat: {e}")
            traceback.print_exc()
    
    print("\n✅ Simulation terminée")

# Exécuter la simulation
simulate_process_order(data)

# Afficher des suggestions pour corriger le problème
print("\n=== Suggestions pour corriger le problème ===")
print("1. Vérifier les permissions de la base de données")
print("2. Vérifier les contraintes d'unicité dans le modèle Order")
print("3. Examiner les logs du serveur Django pour voir l'erreur exacte")
print("4. Vérifier si le fournisseur 'R7' existe déjà dans la base de données")
print("5. Créer un script qui utilise le modèle Django directement pour tester l'insertion d'un achat")