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
        
    return jsonify({
        'spark_results': spark_results,
        'live_api': load_json('live_api.json'),
        'live_rss': load_json('live_rss.json')
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
