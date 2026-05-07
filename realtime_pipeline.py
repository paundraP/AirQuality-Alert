import time
import requests
import json
import random
from datetime import datetime
import pandas as pd
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), 'dashboard', 'data')
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

def get_live_data():
    url = "https://ispu.kemenlh.go.id/apimobile/v1/getStations"
    try:
        res = requests.get(url, timeout=10).json()
    except Exception as e:
        print("Gagal mengambil stasiun:", e)
        return []
    
    daerah = []
    kota_terpakai = set()
    for s in res.get("rows", []):
        if s.get("provinsi") == "Jawa Timur":
            nama = s.get("kota", "").replace("Kota ", "").replace("Kabupaten ", "").strip().lower()
            sid = s.get("id_stasiun")
            if sid and nama not in kota_terpakai:
                kota_terpakai.add(nama)
                daerah.append(sid)
                
    live_api = []
    for sid in daerah:
        d_url = f"https://ispu.kemenlh.go.id/apimobile/v1/getDetail/stasiun/{sid}"
        try:
            d_res = requests.get(d_url, timeout=10).json()
            if "rows" in d_res and len(d_res["rows"]) > 0:
                row = d_res["rows"][0]
                base_aqi = int(row.get("aqi", 0))
                fluctuation = random.randint(-2, 2)
                sim_aqi = max(0, base_aqi + fluctuation)
                
                cat_obj = row.get("kategori", {})
                cat_str = cat_obj.get("nilai", "") if isinstance(cat_obj, dict) else str(cat_obj)
                
                live_api.append({
                    "city": row.get("kota", "").replace("Kota ", "").replace("Kabupaten ", "").strip().title(),
                    "aqi": sim_aqi,
                    "category": cat_str,
                    "timestamp": datetime.now().isoformat(),
                    "pm25": row.get("pm25")
                })
        except Exception as e:
            print(f"Gagal mengambil detail untuk stasiun {sid}:", e)
            
    # Urutkan berdasarkan AQI tertinggi
    live_api.sort(key=lambda x: x["aqi"], reverse=True)
    return live_api

def run_analysis(live_api):
    if not live_api:
        return {}
    df = pd.DataFrame(live_api)
    
    worst = df.sort_values("aqi", ascending=False).head(10)
    worst_list = []
    for i, r in enumerate(worst.to_dict("records")):
        worst_list.append({
            "rank": i+1,
            "city": r["city"],
            "avg_aqi": float(r["aqi"])
        })
        
    dist_list = []
    for c in df['city'].unique():
        cdf = df[df['city'] == c]
        dist_list.append({
            "city": c,
            "baik": len(cdf[cdf['category'].str.lower() == 'baik']),
            "sedang": len(cdf[cdf['category'].str.lower() == 'sedang']),
            "tidak_sehat": len(cdf[cdf['category'].str.lower() == 'tidak sehat']),
            "berbahaya": len(cdf[cdf['category'].str.lower() == 'berbahaya'])
        })
        
    return {
        "worst_cities": worst_list,
        "aqi_distribution": dist_list
    }

if __name__ == "__main__":
    print("=== Memulai Realtime Pipeline (API Fetcher + Analisis Local) ===")
    while True:
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Mengambil data terbaru dari API...")
        try:
            live_api = get_live_data()
            if live_api:
                api_path = os.path.join(DATA_DIR, "live_api.json")
                with open(api_path, "w") as f:
                    json.dump(live_api, f, indent=4)
                print(f"Sukses! {len(live_api)} data stasiun berhasil diperbarui di {api_path}")
                
                spark_results = run_analysis(live_api)
                spark_path = os.path.join(DATA_DIR, "spark_results.json")
                with open(spark_path, "w") as f:
                    json.dump(spark_results, f, indent=4)
                print(f"Sukses! Hasil analisis berhasil diperbarui di {spark_path}")
            else:
                print("Peringatan: Tidak ada data stasiun yang diambil.")
                
        except Exception as e:
            print("Error: Terjadi kesalahan pada pipeline:", e)
            
        print("Menunggu 15 detik sebelum pengambilan selanjutnya...")
        time.sleep(15)
