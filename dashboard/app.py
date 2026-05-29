import os
import json
from flask import Flask, render_template, jsonify

app = Flask(__name__)

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')

def load_json(filename):
    filepath = os.path.join(DATA_DIR, filename)
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
            return data
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def load_latest_batch(dirname):
    directory = os.path.join(DATA_DIR, dirname)
    if not os.path.exists(directory):
        return []

    files = [
        f for f in os.listdir(directory)
        if f.startswith('data_') and f.endswith('.json')
    ]
    if not files:
        return []

    files.sort(reverse=True)
    try:
        with open(os.path.join(directory, files[0]), 'r') as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except (OSError, json.JSONDecodeError):
        return []


def load_all_batches(dirname):
    directory = os.path.join(DATA_DIR, dirname)
    if not os.path.exists(directory):
        return []

    files = sorted(
        [f for f in os.listdir(directory)
         if f.startswith('data_') and f.endswith('.json')],
        reverse=True
    )
    seen = set()
    all_items = []
    for fname in files:
        try:
            with open(os.path.join(directory, fname), 'r') as f:
                data = json.load(f)
                if not isinstance(data, list):
                    continue
                for item in data:
                    title = (item.get('title') or item.get('judul') or '').strip().lower()
                    link = (item.get('link') or '').strip()
                    key = title or link or json.dumps(item, sort_keys=True)
                    if key not in seen:
                        seen.add(key)
                        all_items.append(item)
        except (OSError, json.JSONDecodeError):
            continue
    return all_items

def normalize_live_api(rows):
    normalized = []
    for row in rows:
        city = row.get('city') or row.get('kota') or row.get('id_stasiun') or ''
        category = row.get('category') or row.get('kategori') or ''
        normalized.append({
            'city': str(city).title(),
            'aqi': row.get('aqi'),
            'category': category,
            'timestamp': row.get('timestamp') or row.get('ingested_at') or '',
            'pm25': row.get('pm25')
        })

    normalized.sort(key=lambda item: float(item.get('aqi') or 0), reverse=True)
    return normalized


def aqi_category(aqi_val):
    try:
        aqi = float(aqi_val)
    except (TypeError, ValueError):
        return ''
    if aqi <= 50:
        return 'BAIK'
    if aqi <= 100:
        return 'SEDANG'
    if aqi <= 200:
        return 'TIDAK SEHAT'
    return 'BERBAHAYA'


def load_latest_per_city(dirname):
    directory = os.path.join(DATA_DIR, dirname)
    if not os.path.exists(directory):
        return {}

    files = sorted(
        [f for f in os.listdir(directory)
         if f.startswith('data_') and f.endswith('.json')],
        reverse=True
    )
    latest = {}
    for fname in files:
        try:
            with open(os.path.join(directory, fname), 'r') as f:
                data = json.load(f)
                if not isinstance(data, list):
                    continue
                for item in data:
                    city = str(item.get('city') or item.get('kota') or item.get('id_stasiun') or '').title()
                    if city and city not in latest:
                        latest[city] = item
        except (OSError, json.JSONDecodeError):
            continue
    return latest

def normalize_live_rss(rows):
    return [{
        'title': item.get('title') or item.get('judul', ''),
        'link': item.get('link', ''),
        'published': item.get('published') or item.get('waktu_terbit', ''),
        'summary': item.get('summary') or item.get('ringkasan', '')
    } for item in rows]

def enrich_spark_results(results):
    if not isinstance(results, dict):
        return {}

    enriched = dict(results)

    if 'worst_cities' not in enriched and isinstance(results.get('ranking_kota'), list):
        enriched['worst_cities'] = [{
            'rank': item.get('peringkat') or item.get('rank'),
            'city': str(item.get('kota') or item.get('city') or '').title(),
            'avg_aqi': item.get('avg_aqi')
        } for item in results['ranking_kota']]

    if 'aqi_distribution' not in enriched and isinstance(results.get('distribusi_kategori'), list):
        by_city = {}
        for item in results['distribusi_kategori']:
            city = str(item.get('kota') or item.get('city') or '').title()
            category = str(item.get('kategori') or item.get('category') or '').lower().replace(' ', '_')
            count = int(item.get('jumlah_data') or item.get('count') or item.get('total') or item.get('persentase') or 0)
            if not city:
                continue
            bucket = by_city.setdefault(city, {
                'city': city,
                'baik': 0,
                'sedang': 0,
                'tidak_sehat': 0,
                'berbahaya': 0
            })
            if 'tidak' in category and 'sehat' in category:
                bucket['tidak_sehat'] += count
            elif 'berbahaya' in category:
                bucket['berbahaya'] += count
            elif 'sedang' in category:
                bucket['sedang'] += count
            else:
                bucket['baik'] += count
        enriched['aqi_distribution'] = list(by_city.values())

    return enriched

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def get_data():
    spark_results = enrich_spark_results(load_json('spark_results.json'))

    live_api = normalize_live_api(load_latest_batch('api'))
    if not live_api:
        live_api = normalize_live_api(load_json('live_api.json'))

    historical = load_latest_per_city('api')

    live_cities = {item['city'] for item in live_api}
    ranking = spark_results.get('ranking_kota') or spark_results.get('worst_cities') or []
    for entry in ranking:
        city = str(entry.get('kota') or entry.get('city') or '').title()
        if city and city not in live_cities:
            aqi = entry.get('avg_aqi')
            hist = historical.get(city, {})
            category = hist.get('category') or hist.get('kategori') or aqi_category(aqi)
            timestamp = hist.get('timestamp') or hist.get('ingested_at') or spark_results.get('generated_at', '')
            live_api.append({
                'city': city,
                'aqi': aqi,
                'category': category,
                'timestamp': timestamp,
                'pm25': hist.get('pm25')
            })
    live_api.sort(key=lambda item: float(item.get('aqi') or 0), reverse=True)

    live_rss = normalize_live_rss(load_all_batches('rss'))
    if not live_rss:
        live_rss = normalize_live_rss(load_json('live_rss.json'))

    return jsonify({
        'spark_results': spark_results,
        'live_api': live_api,
        'live_rss': live_rss
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
