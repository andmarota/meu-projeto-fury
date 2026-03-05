from flask import Flask, request, jsonify, render_template
from google.cloud import bigquery

app = Flask(__name__)

client = bigquery.Client(project='meli-bi-data')


@app.route('/ping')
def ping():
    return 'pong', 200


@app.route('/')
def home():
    return 'Meu servico Fury esta funcionando!', 200


@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')


@app.route('/dashboard2')
def dashboard2():
    return render_template('dashboard2.html')


@app.route('/datasets')
def datasets():
    try:
        result = client.list_datasets()
        names = [ds.dataset_id for ds in result]
        return jsonify({'datasets': names, 'total': len(names)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/schema/<dataset>/<table>')
def schema(dataset, table):
    try:
        ref = client.get_table(f'meli-bi-data.{dataset}.{table}')
        fields = [
            {'name': f.name, 'type': f.field_type, 'mode': f.mode}
            for f in ref.schema
        ]
        return jsonify({'table': f'{dataset}.{table}', 'fields': fields, 'total': len(fields)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/summary', methods=['POST'])
def summary():
    body = request.get_json()
    if not body or 'table' not in body:
        return jsonify({'error': 'Campo "table" obrigatorio (ex: WHOWNER.BT_CCARD_CADASTRAL)'}), 400

    table = body['table']
    limit = body.get('limit', 10000)

    try:
        ref = client.get_table(f'meli-bi-data.{table}')
        columns = [f.name for f in ref.schema]

        stats = []
        for col in columns:
            sql = f"""
                SELECT
                    '{col}' AS column_name,
                    COUNT(*) AS total_rows,
                    COUNTIF(`{col}` IS NULL) AS null_count,
                    COUNT(DISTINCT `{col}`) AS distinct_count
                FROM `meli-bi-data.{table}`
                LIMIT {limit}
            """
            rows = list(client.query(sql).result())
            if rows:
                row = dict(rows[0])
                row['null_pct'] = round(row['null_count'] / row['total_rows'] * 100, 2) if row['total_rows'] else 0
                stats.append(row)

        return jsonify({'table': table, 'columns': stats})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/query', methods=['POST'])
def query():
    body = request.get_json()
    if not body or 'sql' not in body:
        return jsonify({'error': 'Campo "sql" obrigatorio'}), 400

    sql = body['sql']
    try:
        query_job = client.query(sql)
        results = query_job.result()
        rows = [dict(row) for row in results]
        return jsonify({'rows': rows, 'total': len(rows)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
