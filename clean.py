#!/usr/bin/env python3
"""Script de nettoyage SQL direct"""

import os
from dotenv import load_dotenv

load_dotenv()

# Liste complète des pharmacies à supprimer (extraite du script multi-pharmacies)
PHARMACIES_TO_CLEAN = [
    "712006733",  # Pharmacie Puig Léveillé
    "062044623",  # GIE Grande Pharmacie Cap 3000
    "692039340",  # Grande Pharmacie de La Part DIEU
    "372006049",  # Pharmacie Sirvin
    "342030285",  # Pharmacie du Centre
    "132069444",  # Pharmacie de la Montagnette
    "132040585",  # Pharmacie du Cours Mirabeau
    "642013593",  # Pharmacie BAB2
    "842004863",  # Pharmacie Becker Monteux
    "132061276",  # Pharmacie du 8 mai 1945
    "342026218",  # Pharmacie Espace Bocaud
    "772012522",  # Pharmacie Val D'Europe
    "422027524",  # Pharmacie de Monthieu
    "682020763",  # Pharmacie de la Croisière
    "262071004",  # Pharmacie Valence 2
    "132086612",  # Pharmacie Martinet
    "832002810",  # Pharmacie Mouysset
    "332018811",  # Pharmacie du Chemin Long
    "332022755",  # Pharmacie de l'Etoile
    "752040428",  # Pharmacie de la Place de la République
    "132066978",  # Pharmacie Centrale
    "832011373",  # Pharmacie Varoise
    "302003330",  # Pharmacie de Castanet
    "342027828",  # SELARL Pharmacie des Arceaux
    "672033586",  # Pharmacie du Printemps
    "202041711",  # Pharmacie Taddei Medori
    "o62037049",  # Pharmacie Lingostière
    "132028473",  # Pharmacie Saint Jean
    "302007638",  # Pharmacie des Portes d'Uzès
    "912015492",  # Pharmacie Centrale Evry 2
    "192005940",  # Pharmacie Egletons
    "302006192",  # Pharmacie des Salicornes
    "952700268",  # Pharmacie Coté Seine
    "202021481",  # Pharmacie du Valinco
    "772011623",  # Pharmacie du Centre Dammarie Les Lys
    "332022219",  # Pharmacie de l'Alliance
    "852007137",  # Pharmacie Ylium
    "422027854",  # Pharmacie du Forez
    "832011498",  # Grande pharmacie hyéroise / Pharmacie Massillon
    "732002811",  # Pharmacie du Pradian
    "422026542",  # Pharmacie de l'Europe
    "922020771",  # Grande Pharmacie de la Station
    "742005481",  # Pharmacie du Leman
    "o52702370",  # Pharmacie de Tokoro
    "922021373",  # Pharmacie des Quatres Chemins
    "952701043",  # Pharmacie de la Muette
    "752043471",  # Pharmacie Faubourg Bastille
    "912015948",  # Grande Pharmacie de Fleury
    "442002119",  # Pharmacie de Beaulieu
    "792020646",  # Pharmacie du Bocage
    "202040697",  # Pharmacie de la Rocade
    "312008915",  # Grande Pharmacie des Arcades
    "692013469",  # Pharmacie Jalles
    "342027588",  # Pharmacie de Capestang
    "842005456",  # Pharmacie de la Sorgue
    "842002008",  # Pharmacie Becker Carpentras
    "842003121",  # Pharmacie de l'Ecluse
    "202021648",  # Pharmacie de Sarrola
    "132081613",  # Pharmacie des Ateliers
    "842006348",  # Pharmacie de Caumont
    "132048687",  # Pharmacie du Centre (Ventabren)
    "732003132",  # Pharmacie des Cascades
    "662004100",  # Pharmacie du Wahoo
    "662004522",  # Pharmacie du Château
    "782712756",  # Pharmacie de la Gare
    "842006462",  # Grande Pharmacie des Ocres
    "342030137",  # Pharmacie Montarnaud
    "280003641",  # Pharmacie du Géant Luce
    "802006031",  # Pharmacie Paque
    "842005472",  # Pharmacie Cap sud
    "132046384",  # Pharmacie du Stade Vélodrome
]

def clean_pharmacy_sql(pharmacy_id: str):
    """Génère les requêtes SQL de suppression pour une pharmacie"""
    sql_commands = f"""
-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '{pharmacy_id}'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '{pharmacy_id}')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '{pharmacy_id}');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '{pharmacy_id}');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '{pharmacy_id}')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '{pharmacy_id}');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '{pharmacy_id}';

"""
    return sql_commands

def main():
    print("Génération du script de nettoyage SQL...")
    
    with open('clean_pharmacies.sql', 'w', encoding='utf-8') as f:
        f.write("-- Script de nettoyage des pharmacies multi-pharmacies\n")
        f.write("-- Généré automatiquement - ATTENTION: Suppression définitive\n")
        f.write(f"-- {len(PHARMACIES_TO_CLEAN)} pharmacies à supprimer\n\n")
        
        f.write("BEGIN;\n\n")
        
        for i, pharmacy_id in enumerate(PHARMACIES_TO_CLEAN, 1):
            f.write(f"-- [{i}/{len(PHARMACIES_TO_CLEAN)}] Nettoyage pharmacie {pharmacy_id}\n")
            f.write(clean_pharmacy_sql(pharmacy_id))
            f.write("\n")
        
        f.write("COMMIT;\n")
    
    print(f"Script SQL généré: clean_pharmacies.sql")
    print(f"Pharmacies à supprimer: {len(PHARMACIES_TO_CLEAN)}")
    print("\nPour exécuter sur le serveur:")
    print("1. Copier le fichier sur le serveur AWS")
    print("2. Exécuter: sudo docker exec -it django python manage.py dbshell < clean_pharmacies.sql")
    print("\nATTENTION: Cette opération supprime définitivement toutes les données!")

if __name__ == "__main__":
    main()