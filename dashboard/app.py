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

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def get_data():
    spark_results = load_json('spark_results.json')
    if isinstance(spark_results, list) and not spark_results:
        spark_results = {}
        
    live_rss = []
    rss_dir = os.path.join(DATA_DIR, 'rss')
    if os.path.exists(rss_dir):
        files = [f for f in os.listdir(rss_dir) if f.startswith('data_') and f.endswith('.json')]
        if files:
            files.sort(reverse=True)
            latest_file = files[0]
            try:
                with open(os.path.join(rss_dir, latest_file), 'r') as f:
                    raw_rss = json.load(f)
                    for item in raw_rss:
                        live_rss.append({
                            'title': item.get('judul', ''),
                            'link': item.get('link', ''),
                            'published': item.get('waktu_terbit', ''),
                            'summary': item.get('ringkasan', '')
                        })
            except Exception as e:
                print(f"Error loading RSS: {e}")
                pass
                
    if not live_rss:
        live_rss = load_json('live_rss.json')

    return jsonify({
        'spark_results': spark_results,
        'live_api': load_json('live_api.json'),
        'live_rss': live_rss
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
