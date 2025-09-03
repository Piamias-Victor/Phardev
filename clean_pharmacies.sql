-- Script de nettoyage des pharmacies multi-pharmacies
-- Généré automatiquement - ATTENTION: Suppression définitive
-- 71 pharmacies à supprimer

BEGIN;

-- [1/71] Nettoyage pharmacie 712006733

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '712006733'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '712006733')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '712006733');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '712006733');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '712006733')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '712006733');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '712006733';


-- [2/71] Nettoyage pharmacie 062044623

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '062044623'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '062044623')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '062044623');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '062044623');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '062044623')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '062044623');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '062044623';


-- [3/71] Nettoyage pharmacie 692039340

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '692039340'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '692039340')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '692039340');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '692039340');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '692039340')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '692039340');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '692039340';


-- [4/71] Nettoyage pharmacie 372006049

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '372006049'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '372006049')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '372006049');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '372006049');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '372006049')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '372006049');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '372006049';


-- [5/71] Nettoyage pharmacie 342030285

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '342030285'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342030285')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342030285');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342030285');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342030285')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342030285');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '342030285';


-- [6/71] Nettoyage pharmacie 132069444

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '132069444'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132069444')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132069444');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132069444');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132069444')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132069444');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '132069444';


-- [7/71] Nettoyage pharmacie 132040585

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '132040585'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132040585')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132040585');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132040585');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132040585')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132040585');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '132040585';


-- [8/71] Nettoyage pharmacie 642013593

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '642013593'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '642013593')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '642013593');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '642013593');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '642013593')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '642013593');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '642013593';


-- [9/71] Nettoyage pharmacie 842004863

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '842004863'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842004863')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842004863');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842004863');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842004863')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842004863');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '842004863';


-- [10/71] Nettoyage pharmacie 132061276

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '132061276'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132061276')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132061276');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132061276');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132061276')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132061276');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '132061276';


-- [11/71] Nettoyage pharmacie 342026218

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '342026218'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342026218')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342026218');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342026218');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342026218')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342026218');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '342026218';


-- [12/71] Nettoyage pharmacie 772012522

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '772012522'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '772012522')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '772012522');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '772012522');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '772012522')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '772012522');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '772012522';


-- [13/71] Nettoyage pharmacie 422027524

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '422027524'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '422027524')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '422027524');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '422027524');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '422027524')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '422027524');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '422027524';


-- [14/71] Nettoyage pharmacie 682020763

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '682020763'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '682020763')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '682020763');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '682020763');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '682020763')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '682020763');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '682020763';


-- [15/71] Nettoyage pharmacie 262071004

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '262071004'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '262071004')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '262071004');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '262071004');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '262071004')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '262071004');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '262071004';


-- [16/71] Nettoyage pharmacie 132086612

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '132086612'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132086612')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132086612');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132086612');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132086612')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132086612');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '132086612';


-- [17/71] Nettoyage pharmacie 832002810

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '832002810'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '832002810')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '832002810');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '832002810');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '832002810')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '832002810');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '832002810';


-- [18/71] Nettoyage pharmacie 332018811

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '332018811'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '332018811')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '332018811');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '332018811');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '332018811')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '332018811');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '332018811';


-- [19/71] Nettoyage pharmacie 332022755

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '332022755'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '332022755')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '332022755');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '332022755');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '332022755')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '332022755');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '332022755';


-- [20/71] Nettoyage pharmacie 752040428

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '752040428'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '752040428')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '752040428');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '752040428');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '752040428')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '752040428');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '752040428';


-- [21/71] Nettoyage pharmacie 132066978

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '132066978'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132066978')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132066978');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132066978');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132066978')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132066978');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '132066978';


-- [22/71] Nettoyage pharmacie 832011373

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '832011373'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '832011373')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '832011373');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '832011373');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '832011373')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '832011373');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '832011373';


-- [23/71] Nettoyage pharmacie 302003330

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '302003330'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '302003330')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '302003330');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '302003330');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '302003330')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '302003330');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '302003330';


-- [24/71] Nettoyage pharmacie 342027828

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '342027828'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342027828')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342027828');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342027828');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342027828')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342027828');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '342027828';


-- [25/71] Nettoyage pharmacie 672033586

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '672033586'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '672033586')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '672033586');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '672033586');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '672033586')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '672033586');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '672033586';


-- [26/71] Nettoyage pharmacie 202041711

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '202041711'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '202041711')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '202041711');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '202041711');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '202041711')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '202041711');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '202041711';


-- [27/71] Nettoyage pharmacie o62037049

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = 'o62037049'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = 'o62037049')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = 'o62037049');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = 'o62037049');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = 'o62037049')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = 'o62037049');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = 'o62037049';


-- [28/71] Nettoyage pharmacie 132028473

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '132028473'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132028473')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132028473');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132028473');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132028473')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132028473');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '132028473';


-- [29/71] Nettoyage pharmacie 302007638

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '302007638'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '302007638')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '302007638');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '302007638');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '302007638')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '302007638');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '302007638';


-- [30/71] Nettoyage pharmacie 912015492

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '912015492'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '912015492')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '912015492');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '912015492');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '912015492')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '912015492');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '912015492';


-- [31/71] Nettoyage pharmacie 192005940

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '192005940'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '192005940')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '192005940');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '192005940');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '192005940')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '192005940');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '192005940';


-- [32/71] Nettoyage pharmacie 302006192

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '302006192'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '302006192')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '302006192');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '302006192');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '302006192')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '302006192');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '302006192';


-- [33/71] Nettoyage pharmacie 952700268

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '952700268'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '952700268')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '952700268');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '952700268');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '952700268')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '952700268');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '952700268';


-- [34/71] Nettoyage pharmacie 202021481

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '202021481'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '202021481')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '202021481');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '202021481');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '202021481')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '202021481');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '202021481';


-- [35/71] Nettoyage pharmacie 772011623

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '772011623'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '772011623')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '772011623');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '772011623');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '772011623')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '772011623');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '772011623';


-- [36/71] Nettoyage pharmacie 332022219

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '332022219'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '332022219')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '332022219');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '332022219');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '332022219')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '332022219');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '332022219';


-- [37/71] Nettoyage pharmacie 852007137

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '852007137'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '852007137')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '852007137');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '852007137');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '852007137')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '852007137');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '852007137';


-- [38/71] Nettoyage pharmacie 422027854

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '422027854'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '422027854')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '422027854');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '422027854');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '422027854')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '422027854');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '422027854';


-- [39/71] Nettoyage pharmacie 832011498

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '832011498'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '832011498')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '832011498');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '832011498');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '832011498')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '832011498');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '832011498';


-- [40/71] Nettoyage pharmacie 732002811

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '732002811'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '732002811')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '732002811');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '732002811');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '732002811')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '732002811');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '732002811';


-- [41/71] Nettoyage pharmacie 422026542

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '422026542'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '422026542')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '422026542');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '422026542');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '422026542')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '422026542');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '422026542';


-- [42/71] Nettoyage pharmacie 922020771

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '922020771'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '922020771')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '922020771');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '922020771');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '922020771')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '922020771');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '922020771';


-- [43/71] Nettoyage pharmacie 742005481

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '742005481'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '742005481')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '742005481');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '742005481');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '742005481')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '742005481');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '742005481';


-- [44/71] Nettoyage pharmacie o52702370

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = 'o52702370'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = 'o52702370')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = 'o52702370');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = 'o52702370');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = 'o52702370')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = 'o52702370');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = 'o52702370';


-- [45/71] Nettoyage pharmacie 922021373

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '922021373'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '922021373')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '922021373');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '922021373');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '922021373')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '922021373');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '922021373';


-- [46/71] Nettoyage pharmacie 952701043

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '952701043'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '952701043')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '952701043');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '952701043');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '952701043')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '952701043');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '952701043';


-- [47/71] Nettoyage pharmacie 752043471

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '752043471'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '752043471')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '752043471');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '752043471');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '752043471')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '752043471');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '752043471';


-- [48/71] Nettoyage pharmacie 912015948

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '912015948'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '912015948')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '912015948');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '912015948');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '912015948')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '912015948');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '912015948';


-- [49/71] Nettoyage pharmacie 442002119

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '442002119'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '442002119')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '442002119');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '442002119');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '442002119')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '442002119');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '442002119';


-- [50/71] Nettoyage pharmacie 792020646

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '792020646'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '792020646')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '792020646');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '792020646');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '792020646')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '792020646');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '792020646';


-- [51/71] Nettoyage pharmacie 202040697

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '202040697'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '202040697')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '202040697');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '202040697');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '202040697')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '202040697');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '202040697';


-- [52/71] Nettoyage pharmacie 312008915

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '312008915'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '312008915')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '312008915');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '312008915');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '312008915')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '312008915');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '312008915';


-- [53/71] Nettoyage pharmacie 692013469

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '692013469'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '692013469')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '692013469');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '692013469');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '692013469')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '692013469');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '692013469';


-- [54/71] Nettoyage pharmacie 342027588

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '342027588'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342027588')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342027588');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342027588');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342027588')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342027588');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '342027588';


-- [55/71] Nettoyage pharmacie 842005456

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '842005456'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842005456')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842005456');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842005456');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842005456')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842005456');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '842005456';


-- [56/71] Nettoyage pharmacie 842002008

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '842002008'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842002008')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842002008');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842002008');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842002008')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842002008');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '842002008';


-- [57/71] Nettoyage pharmacie 842003121

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '842003121'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842003121')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842003121');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842003121');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842003121')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842003121');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '842003121';


-- [58/71] Nettoyage pharmacie 202021648

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '202021648'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '202021648')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '202021648');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '202021648');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '202021648')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '202021648');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '202021648';


-- [59/71] Nettoyage pharmacie 132081613

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '132081613'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132081613')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132081613');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132081613');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132081613')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132081613');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '132081613';


-- [60/71] Nettoyage pharmacie 842006348

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '842006348'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842006348')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842006348');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842006348');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842006348')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842006348');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '842006348';


-- [61/71] Nettoyage pharmacie 132048687

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '132048687'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132048687')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132048687');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132048687');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132048687')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132048687');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '132048687';


-- [62/71] Nettoyage pharmacie 732003132

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '732003132'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '732003132')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '732003132');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '732003132');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '732003132')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '732003132');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '732003132';


-- [63/71] Nettoyage pharmacie 662004100

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '662004100'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '662004100')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '662004100');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '662004100');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '662004100')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '662004100');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '662004100';


-- [64/71] Nettoyage pharmacie 662004522

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '662004522'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '662004522')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '662004522');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '662004522');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '662004522')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '662004522');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '662004522';


-- [65/71] Nettoyage pharmacie 782712756

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '782712756'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '782712756')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '782712756');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '782712756');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '782712756')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '782712756');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '782712756';


-- [66/71] Nettoyage pharmacie 842006462

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '842006462'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842006462')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842006462');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842006462');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842006462')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842006462');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '842006462';


-- [67/71] Nettoyage pharmacie 342030137

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '342030137'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342030137')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342030137');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342030137');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342030137')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '342030137');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '342030137';


-- [68/71] Nettoyage pharmacie 280003641

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '280003641'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '280003641')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '280003641');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '280003641');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '280003641')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '280003641');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '280003641';


-- [69/71] Nettoyage pharmacie 802006031

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '802006031'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '802006031')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '802006031');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '802006031');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '802006031')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '802006031');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '802006031';


-- [70/71] Nettoyage pharmacie 842005472

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '842005472'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842005472')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842005472');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842005472');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842005472')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '842005472');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '842005472';


-- [71/71] Nettoyage pharmacie 132046384

-- Suppression des ventes
DELETE FROM data_sales 
WHERE product_id IN (
    SELECT ds.id FROM data_inventorysnapshot ds
    JOIN data_internalproduct dp ON ds.product_id = dp.id
    JOIN data_pharmacy ph ON dp.pharmacy_id = ph.id
    WHERE ph.id_nat = '132046384'
);

-- Suppression des produits-commandes
DELETE FROM data_productorder 
WHERE order_id IN (
    SELECT id FROM data_order 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132046384')
);

-- Suppression des commandes
DELETE FROM data_order 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132046384');

-- Suppression des fournisseurs (AJOUTÉ)
DELETE FROM data_supplier 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132046384');

-- Suppression des snapshots d'inventaire
DELETE FROM data_inventorysnapshot 
WHERE product_id IN (
    SELECT id FROM data_internalproduct 
    WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132046384')
);

-- Suppression des produits internes
DELETE FROM data_internalproduct 
WHERE pharmacy_id = (SELECT id FROM data_pharmacy WHERE id_nat = '132046384');

-- Suppression de la pharmacie
DELETE FROM data_pharmacy WHERE id_nat = '132046384';


COMMIT;
