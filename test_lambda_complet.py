#!/usr/bin/env python3
"""
Test de structure des données API Winpharma
Ce script analyse les données récupérées pour vérifier leur compatibilité avec le backend Django
"""

import json
import os
import sys

# Définition des structures attendues par le backend Django
EXPECTED_STRUCTURES = {
    "produits": {
        "block_key": "produits",
        "required_fields": ["ProdId", "Nom", "Stock", "PrixTTC", "PrixMP"],
        "optional_fields": ["Code13Ref", "TVA", "ExtraCodes"],
        "description": "Liste des produits avec leurs caractéristiques"
    },
    "achats": {
        "block_key": "achats",
        "required_fields": ["id", "codeFourn", "nomFourn", "lignes"],
        "optional_fields": ["channel", "dateEnvoi", "dateLivraison", "typeFourn"],
        "line_required_fields": ["prodId", "qteC", "qteR"],
        "line_optional_fields": ["qteUG", "qteEC", "code13Ref", "extraCodes", "nomProduit", "prix", "remiseFinal"],
        "description": "Liste des commandes avec leurs lignes de produits"
    },
    "ventes": {
        "block_key": "ventes",
        "required_fields": ["id", "heure", "lignes"],
        "optional_fields": ["codeOperateur", "codeClient", "typeClient", "agePatient", 
                           "codePrescripteur", "specPrescripteur", "dateOrdonnance", 
                           "dateFacture", "typeVente", "totalTTC", "totalAMO", "totalAMC"],
        "line_required_fields": ["prodId", "qte"],
        "line_optional_fields": ["code13Ref", "nomProduit", "pourcentRemise", "tva", "codeActe", 
                                "prix", "PA_HT", "PV_HT", "PV_TTC", "montantTTC", "location", 
                                "ordo", "retrocession"],
        "description": "Liste des ventes avec leurs lignes de produits"
    }
}

# Fonctions d'analyse
def analyze_file(filename, expected):
    """Analyse un fichier JSON pour vérifier sa structure"""
    print(f"\n===== Analyse de {filename} =====")
    
    # Vérifier si le fichier existe
    if not os.path.exists(filename):
        print(f"❌ Le fichier {filename} n'existe pas")
        return False
    
    # Charger les données
    try:
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"❌ Erreur lors de la lecture du fichier: {e}")
        return False
    
    # Vérifier si les données sont vides
    if not data:
        print("❌ Les données sont vides")
        return False
    
    # Vérifier la structure de premier niveau
    block_key = expected["block_key"]
    blocks_with_key = [block for block in data if block_key in block]
    
    if not blocks_with_key:
        print(f"❌ Aucun bloc ne contient la clé '{block_key}'")
        print(f"Clés disponibles: {list(data[0].keys()) if data else 'Aucune'}")
        return False
    
    print(f"✅ Structure de premier niveau OK (clé '{block_key}' trouvée)")
    
    # Vérifier les métadonnées des blocs
    for block in data:
        if "cip_pharma" in block:
            print(f"ℹ️ Pharmacie ID: {block['cip_pharma']}")
    
    # Compter les éléments
    element_count = sum(len(block[block_key]) for block in blocks_with_key)
    print(f"✅ {element_count} éléments trouvés dans '{block_key}'")
    
    # Vérifier la structure d'un élément
    items = []
    for block in blocks_with_key:
        items.extend(block[block_key])
    
    if not items:
        print(f"❌ Aucun élément dans '{block_key}'")
        return False
    
    # Analyser le premier élément comme exemple
    sample = items[0]
    
    print("\nChamps disponibles dans un exemple:")
    for key, value in sample.items():
        # Tronquer les valeurs longues
        if isinstance(value, str) and len(value) > 50:
            display_value = value[:47] + "..."
        elif isinstance(value, list) and len(str(value)) > 50:
            display_value = str(value)[:47] + "..."
        else:
            display_value = value
        print(f"  - {key}: {display_value}")
    
    # Vérifier les champs requis
    missing_required = []
    for field in expected["required_fields"]:
        if field not in sample:
            missing_required.append(field)
    
    if missing_required:
        print(f"\n❌ Champs requis manquants: {missing_required}")
    else:
        print(f"\n✅ Tous les champs requis sont présents")
    
    # Vérifier les champs optionnels
    missing_optional = []
    for field in expected["optional_fields"]:
        if field not in sample:
            missing_optional.append(field)
    
    if missing_optional:
        print(f"⚠️ Champs optionnels manquants: {missing_optional}")
    else:
        print(f"✅ Tous les champs optionnels sont présents")
    
    # Pour les achats et ventes, vérifier les lignes
    if "line_required_fields" in expected:
        if "lignes" not in sample:
            print("❌ Le champ 'lignes' est manquant")
            return False
        
        if not sample["lignes"]:
            print("⚠️ Aucune ligne dans l'exemple")
        else:
            line_sample = sample["lignes"][0]
            print("\nChamps disponibles dans une ligne:")
            for key, value in line_sample.items():
                # Tronquer les valeurs longues
                if isinstance(value, str) and len(value) > 50:
                    display_value = value[:47] + "..."
                elif isinstance(value, list) and len(str(value)) > 50:
                    display_value = str(value)[:47] + "..."
                else:
                    display_value = value
                print(f"  - {key}: {display_value}")
            
            # Vérifier les champs requis des lignes
            missing_line_required = []
            for field in expected["line_required_fields"]:
                if field not in line_sample:
                    missing_line_required.append(field)
            
            if missing_line_required:
                print(f"\n❌ Champs requis manquants dans les lignes: {missing_line_required}")
            else:
                print(f"\n✅ Tous les champs requis des lignes sont présents")
            
            # Vérifier les champs optionnels des lignes
            missing_line_optional = []
            for field in expected["line_optional_fields"]:
                if field not in line_sample:
                    missing_line_optional.append(field)
            
            if missing_line_optional:
                print(f"⚠️ Champs optionnels manquants dans les lignes: {missing_line_optional}")
            else:
                print(f"✅ Tous les champs optionnels des lignes sont présents")
    
    # Vérifier la compatibilité avec le backend Django
    print("\n----- Compatibilité avec le backend Django -----")
    
    # Mapping des endpoints vers les fonctions Django
    django_functions = {
        "produits": "process_product",
        "achats": "process_order",
        "ventes": "process_sales"
    }
    
    endpoint = os.path.splitext(os.path.basename(filename))[0]
    function_name = django_functions.get(endpoint)
    
    if function_name:
        print(f"✅ Fonction de traitement Django: {function_name}")
        print(f"✅ Structure compatible avec le backend Django")
    else:
        print(f"❓ Pas de fonction de traitement Django connue pour {endpoint}")
    
    return True

# Programme principal
def main():
    """Fonction principale"""
    print("ANALYSE DES STRUCTURES DE DONNÉES WINPHARMA 2")
    print("--------------------------------------------")
    
    # Vérifier les fichiers JSON dans le répertoire courant
    json_files = [f for f in os.listdir(".") if f.endswith("_response.json")]
    
    if not json_files:
        print("❌ Aucun fichier JSON trouvé dans le répertoire courant")
        print("Exécutez d'abord le script test_simple_api.py pour générer les fichiers")
        return
    
    # Analyser chaque fichier
    results = {}
    
    for file in json_files:
        endpoint = file.replace("_response.json", "")
        if endpoint in EXPECTED_STRUCTURES:
            success = analyze_file(file, EXPECTED_STRUCTURES[endpoint])
            results[endpoint] = "✅ OK" if success else "❌ Problèmes détectés"
        else:
            print(f"\n⚠️ Structure attendue inconnue pour {file}")
            results[file] = "⚠️ Structure inconnue"
    
    # Afficher le résumé
    print("\n===== RÉSUMÉ =====")
    for endpoint, result in results.items():
        print(f"{endpoint}: {result}")
    
    # Conclusion
    if all(result.startswith("✅") for result in results.values()):
        print("\n✅ TOUTES LES STRUCTURES SONT COMPATIBLES")
        print("Le backend Django devrait pouvoir traiter ces données correctement")
    else:
        print("\n⚠️ CERTAINES STRUCTURES PEUVENT POSER PROBLÈME")
        print("Vérifiez les problèmes signalés avant de continuer")

if __name__ == "__main__":
    main()