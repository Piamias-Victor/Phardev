import json
import os
import sys

"""
Ce script analyse les données JSON récupérées de l'API Winpharma 2
et vérifie si elles sont compatibles avec la structure attendue 
par le code de traitement dans winpharma_2.py.
"""

# Vérifier si les fichiers de résultats existent
if not os.path.exists("resultats_api"):
    print("❌ Le dossier 'resultats_api' n'existe pas. Exécutez d'abord le script de test.")
    sys.exit(1)

# Structure attendue selon le code winpharma_2.py
expected_structure = {
    "produits": {
        "block_key": "produits",
        "required_fields": ["ProdId", "Nom", "Stock", "PrixTTC", "PrixMP"],
        "optional_fields": ["Code13Ref", "TVA"],
        "description": "Liste des produits avec leurs caractéristiques"
    },
    "commandes": {
        "block_key": "achats",
        "required_fields": ["id", "codeFourn", "nomFourn", "lignes"],
        "optional_fields": ["channel", "dateEnvoi", "dateLivraison"],
        "line_required_fields": ["prodId", "qteC", "qteR"],
        "line_optional_fields": ["qteUG", "qteEC"],
        "description": "Liste des commandes avec leurs lignes de produits"
    },
    "ventes": {
        "block_key": "ventes",
        "required_fields": ["heure", "lignes"],
        "optional_fields": [],
        "line_required_fields": ["prodId", "qte"],
        "line_optional_fields": [],
        "description": "Liste des ventes avec leurs lignes de produits"
    }
}

# Fonction pour analyser un fichier JSON
def analyze_file(filename, expected):
    print(f"\n===== Analyse du fichier {filename} =====")
    
    # Vérifier si le fichier existe
    filepath = os.path.join("resultats_api", filename)
    if not os.path.exists(filepath):
        print(f"❌ Le fichier {filepath} n'existe pas")
        return False
    
    # Charger les données
    try:
        with open(filepath, "r", encoding="utf-8") as f:
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
    
    # Vérifier la structure des éléments
    items = []
    for block in blocks_with_key:
        items.extend(block[block_key])
    
    if not items:
        print(f"❌ Aucun élément dans '{block_key}'")
        return False
    
    print(f"✅ {len(items)} éléments trouvés dans '{block_key}'")
    
    # Vérifier les champs requis
    sample = items[0]
    print("\nChamps disponibles dans un exemple:", list(sample.keys()))
    
    missing_required = []
    for field in expected["required_fields"]:
        if field not in sample:
            missing_required.append(field)
    
    if missing_required:
        print(f"❌ Champs requis manquants: {missing_required}")
    else:
        print(f"✅ Tous les champs requis sont présents")
    
    # Vérifier les champs optionnels
    missing_optional = []
    for field in expected["optional_fields"]:
        if field not in sample:
            missing_optional.append(field)
    
    if missing_optional:
        print(f"⚠️ Champs optionnels manquants: {missing_optional}")
    else:
        print(f"✅ Tous les champs optionnels sont présents")
    
    # Pour les commandes et ventes, vérifier les lignes
    if "line_required_fields" in expected:
        if "lignes" not in sample:
            print("❌ Le champ 'lignes' est manquant")
            return False
        
        if not sample["lignes"]:
            print("⚠️ Aucune ligne dans l'exemple")
        else:
            line_sample = sample["lignes"][0]
            print("\nChamps disponibles dans une ligne:", list(line_sample.keys()))
            
            missing_line_required = []
            for field in expected["line_required_fields"]:
                if field not in line_sample:
                    missing_line_required.append(field)
            
            if missing_line_required:
                print(f"❌ Champs requis manquants dans les lignes: {missing_line_required}")
            else:
                print(f"✅ Tous les champs requis des lignes sont présents")
            
            missing_line_optional = []
            for field in expected["line_optional_fields"]:
                if field not in line_sample:
                    missing_line_optional.append(field)
            
            if missing_line_optional:
                print(f"⚠️ Champs optionnels manquants dans les lignes: {missing_line_optional}")
            else:
                print(f"✅ Tous les champs optionnels des lignes sont présents")
    
    return True

# Analyser chaque fichier
for endpoint, expected in expected_structure.items():
    success = analyze_file(f"{endpoint}.json", expected)
    if not success:
        print(f"\n⚠️ Problèmes avec l'endpoint {endpoint}. Des ajustements seront nécessaires.")

# Vérifier les autres fichiers éventuels
for filename in os.listdir("resultats_api"):
    if filename.endswith(".json") and filename not in [f"{k}.json" for k in expected_structure.keys()]:
        print(f"\n===== Fichier supplémentaire trouvé: {filename} =====")
        print("Ce fichier n'est pas attendu par le code actuel. Il peut s'agir d'un nouvel endpoint.")

print("\n===== Résumé =====")
print("Cette analyse permet d'identifier les différences entre la structure attendue par le code")
print("et la structure réelle des données de l'API Winpharma 2.")
print("Utilisez ces informations pour ajuster le code de traitement si nécessaire.")