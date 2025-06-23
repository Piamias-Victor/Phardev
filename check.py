#!/usr/bin/env python3
"""
Comparaison directe API vs DB pour les ventes
"""

import requests
import psycopg2
import os
from collections import defaultdict
from datetime import datetime, timedelta

# Configuration
API_URL = "YXBvdGhpY2Fs"
API_PASSWORD = "cGFzczE" 
PHARMACY_ID = "832011373"
BASE_URL = "https://grpstat.winpharma.com/ApiWp"

# Période de comparaison (même que la Lambda)
dt2 = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')  # 22 juin
dt1 = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')  # 16 juin

print(f"🔍 COMPARAISON VENTES API vs DB")
print(f"📅 Période: {dt1} → {dt2}")
print(f"🏥 Pharmacie: {PHARMACY_ID}")
print(f"=" * 60)

def get_api_sales():
    """Récupère les ventes depuis l'API"""
    url = f"{BASE_URL}/{API_URL}/ventes"
    params = {
        'password': API_PASSWORD,
        'Idnats': PHARMACY_ID,
        'dt1': dt1,
        'dt2': dt2
    }
    
    try:
        print(f"📡 Récupération API...")
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            pharmacy_data = data[0]
            ventes = pharmacy_data.get('ventes', [])
            
            # Analyser les ventes
            api_stats = {
                'total_sales': len(ventes),
                'total_lines': 0,
                'dates': set(),
                'products': set(),
                'quantities_by_date': defaultdict(int),
                'quantities_by_product': defaultdict(int)
            }
            
            for vente in ventes:
                sale_date = vente.get('heure', '')[:10]  # Extract date part
                api_stats['dates'].add(sale_date)
                
                for ligne in vente.get('lignes', []):
                    api_stats['total_lines'] += 1
                    product_id = str(ligne.get('prodId'))
                    quantity = int(ligne.get('qte', 0))
                    
                    api_stats['products'].add(product_id)
                    api_stats['quantities_by_date'][sale_date] += quantity
                    api_stats['quantities_by_product'][product_id] += quantity
            
            print(f"✅ API: {api_stats['total_sales']} ventes, {api_stats['total_lines']} lignes")
            return api_stats
            
        else:
            print(f"❌ API Error {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ API Error: {e}")
        return None

def get_db_sales():
    """Récupère les ventes depuis la DB"""
    try:
        print(f"🗄️ Récupération DB...")
        
        # Connection DB (avec tes vrais paramètres)
        conn = psycopg2.connect(
            host="phardev.cts8s2sgms8l.eu-west-3.rds.amazonaws.com",
            database="postgres",
            user="postgres",
            password="NNPwUUstdTonFYZwfisO",
            port=5432
        )
        
        cursor = conn.cursor()
        
        # Requête pour récupérer les stats
        query = """
        SELECT 
            s.date,
            ip.internal_id,
            SUM(s.quantity) as total_quantity,
            COUNT(*) as nb_records
        FROM data_sales s
        JOIN data_inventorysnapshot inv ON s.product_id = inv.id
        JOIN data_internalproduct ip ON inv.product_id = ip.id  
        JOIN data_pharmacy p ON ip.pharmacy_id = p.id
        WHERE p.id_nat = %s
          AND s.date >= %s 
          AND s.date <= %s
        GROUP BY s.date, ip.internal_id
        ORDER BY s.date, ip.internal_id;
        """
        
        cursor.execute(query, (PHARMACY_ID, dt1, dt2))
        results = cursor.fetchall()
        
        # Analyser les résultats
        db_stats = {
            'total_records': len(results),
            'dates': set(),
            'products': set(),
            'quantities_by_date': defaultdict(int),
            'quantities_by_product': defaultdict(int),
            'total_quantity': 0
        }
        
        for date, product_id, quantity, nb_records in results:
            date_str = date.strftime('%Y-%m-%d')
            product_str = str(product_id)
            
            db_stats['dates'].add(date_str)
            db_stats['products'].add(product_str)
            db_stats['quantities_by_date'][date_str] += quantity
            db_stats['quantities_by_product'][product_str] += quantity
            db_stats['total_quantity'] += quantity
        
        cursor.close()
        conn.close()
        
        print(f"✅ DB: {db_stats['total_records']} records")
        return db_stats
        
    except Exception as e:
        print(f"❌ DB Error: {e}")
        return None

def compare_stats(api_stats, db_stats):
    """Compare les statistiques"""
    print(f"\n📊 COMPARAISON DÉTAILLÉE")
    print(f"-" * 40)
    
    # Comparaison globale
    print(f"🔢 TOTAUX:")
    print(f"   API lignes    : {api_stats['total_lines']}")
    print(f"   DB records    : {db_stats['total_records']}")
    print(f"   Différence    : {api_stats['total_lines'] - db_stats['total_records']}")
    
    # Comparaison des dates
    print(f"\n📅 DATES:")
    api_dates = sorted(api_stats['dates'])
    db_dates = sorted(db_stats['dates'])
    print(f"   API dates     : {api_dates}")
    print(f"   DB dates      : {db_dates}")
    print(f"   Dates manquantes API→DB : {set(api_dates) - set(db_dates)}")
    print(f"   Dates manquantes DB→API : {set(db_dates) - set(api_dates)}")
    
    # Comparaison des produits
    print(f"\n📦 PRODUITS:")
    print(f"   API produits  : {len(api_stats['products'])}")
    print(f"   DB produits   : {len(db_stats['products'])}")
    common_products = set(api_stats['products']) & set(db_stats['products'])
    print(f"   Produits communs : {len(common_products)}")
    
    # Comparaison des quantités par date
    print(f"\n📈 QUANTITÉS PAR DATE:")
    all_dates = sorted(set(api_dates + db_dates))
    for date in all_dates:
        api_qty = api_stats['quantities_by_date'].get(date, 0)
        db_qty = db_stats['quantities_by_date'].get(date, 0)
        diff = api_qty - db_qty
        status = "✅" if diff == 0 else "❌"
        print(f"   {date}: API={api_qty:4d}, DB={db_qty:4d}, Diff={diff:4d} {status}")
    
    # Quelques exemples de produits
    print(f"\n📦 EXEMPLES PRODUITS (premiers 5):")
    sample_products = sorted(common_products)[:5]
    for product in sample_products:
        api_qty = api_stats['quantities_by_product'].get(product, 0)
        db_qty = db_stats['quantities_by_product'].get(product, 0)
        diff = api_qty - db_qty
        status = "✅" if diff == 0 else "❌"
        print(f"   Produit {product}: API={api_qty:3d}, DB={db_qty:3d}, Diff={diff:3d} {status}")

def main():
    # Récupérer les données
    api_stats = get_api_sales()
    db_stats = get_db_sales()
    
    if api_stats and db_stats:
        compare_stats(api_stats, db_stats)
        
        # Conclusion
        print(f"\n🎯 CONCLUSION:")
        api_total = api_stats['total_lines']
        db_total = db_stats['total_records']
        
        if api_total == db_total:
            print(f"✅ PARFAIT! Les données correspondent exactement.")
        else:
            diff = api_total - db_total
            print(f"⚠️ DIFFÉRENCE: {diff} records")
            if diff > 0:
                print(f"   → L'API a {diff} lignes de plus que la DB")
            else:
                print(f"   → La DB a {abs(diff)} lignes de plus que l'API")
    else:
        print(f"❌ Impossible de comparer - erreur de récupération des données")

if __name__ == "__main__":
    main()