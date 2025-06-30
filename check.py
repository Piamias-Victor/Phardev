#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Comparaison API ↔ DB pour la période du 2025-06-23 au 2025-06-24
"""

import requests
import psycopg2
from collections import defaultdict

# --- Configuration ---
API_URL = "YXBvdGhpY2Fs"
API_PASSWORD = "cGFzczE"
PHARMACY_ID = "832011373"
BASE_URL = "https://grpstat.winpharma.com/ApiWp"
DT1 = "2025-06-23"
DT2 = "2025-06-24"

def fetch_api_data(dt1, dt2):
    print(f"📡 APPEL API pour {dt1} → {dt2}")
    try:
        r = requests.get(
            f"{BASE_URL}/{API_URL}/ventes",
            params={
                "password": API_PASSWORD,
                "Idnats": PHARMACY_ID,
                "dt1": dt1,
                "dt2": dt2
            },
            timeout=60
        )
        print(f"→ Status API: {r.status_code}")
        if r.status_code != 200:
            print("❌ Pas de données API") 
            return None

        payload = r.json()
        if not payload:
            print("⚠️ Réponse vide")
            return None

        ventes = payload[0].get("ventes", [])
        print(f"✅ {len(ventes)} ventes reçues")
        lines = 0
        products = set()
        dates = defaultdict(int)

        for sale in ventes:
            d = sale.get("heure", "")[:10]
            dates[d] += 1
            for ligne in sale.get("lignes", []):
                lines += 1
                products.add(str(ligne.get("prodId")))

        return {
            "ventes": len(ventes),
            "lines": lines,
            "products": len(products),
            "dates": dict(dates)
        }

    except Exception as e:
        print(f"💥 Erreur API: {e}")
        return None


def fetch_db_data(dt1, dt2):
    print(f"🗄️ QUERY DB pour {dt1} → {dt2}")
    try:
        conn = psycopg2.connect(
            host="phardev.cts8s2sgms8l.eu-west-3.rds.amazonaws.com",
            database="postgres",
            user="postgres",
            password="NNPwUUstdTonFYZwfisO",
            port=5432
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT s.date, COUNT(*) AS cnt
            FROM data_sales s
            JOIN data_inventorysnapshot inv ON s.product_id = inv.id
            JOIN data_internalproduct ip ON inv.product_id = ip.id
            JOIN data_pharmacy p ON ip.pharmacy_id = p.id
            WHERE p.id_nat = %s AND s.date BETWEEN %s AND %s
            GROUP BY s.date;
        """, (PHARMACY_ID, dt1, dt2))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        total_lines = sum(row[1] for row in rows)
        return {
            "daily_counts": {row[0].isoformat(): row[1] for row in rows},
            "total_lines": total_lines
        }

    except Exception as e:
        print(f"💥 Erreur DB: {e}")
        return None


def compare(api, db):
    print("\n" + "="*40)
    print(f"📊 COMPARAISON sur {DT1} → {DT2}")
    print("="*40)
    if not api:
        print("❌ Aucun résultat API")
    if not db:
        print("❌ Aucun résultat DB")

    if api and db:
        print(f"API → {api['ventes']} ventes, {api['lines']} lignes, {api['products']} produits")
        print("   Répartition par date :", api["dates"])
        print(f"DB → {db['total_lines']} lignes total")
        print("   Répartition par jour  :", db["daily_counts"])

        # Comparaison journalière
        for d in [DT1, DT2]:
            api_count = api["dates"].get(d, 0)
            db_count = db["daily_counts"].get(d, 0)
            status = "✅ OK" if api_count == db_count else "⚠️ Différence!"
            print(f"   {d}: API={api_count} vs DB={db_count} → {status}")
    print("="*40)


if __name__ == "__main__":
    api_data = fetch_api_data(DT1, DT2)
    db_data = fetch_db_data(DT1, DT2)
    compare(api_data, db_data)