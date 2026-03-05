import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture
def client():
    with patch('google.cloud.bigquery.Client'):
        from app import app
        app.config['TESTING'] = True
        with app.test_client() as c:
            yield c


def test_ping(client):
    response = client.get('/ping')
    assert response.status_code == 200
    assert response.data == b'pong'


def test_home(client):
    response = client.get('/')
    assert response.status_code == 200


def test_datasets(client):
    mock_ds = MagicMock()
    mock_ds.dataset_id = 'WHOWNER'

    with patch('app.client') as mock_bq:
        mock_bq.list_datasets.return_value = [mock_ds]
        response = client.get('/datasets')
        assert response.status_code == 200
        data = response.get_json()
        assert 'datasets' in data
        assert 'WHOWNER' in data['datasets']


def test_schema(client):
    mock_field = MagicMock()
    mock_field.name = 'CCARD_ACCOUNT_ID'
    mock_field.field_type = 'STRING'
    mock_field.mode = 'NULLABLE'

    mock_table = MagicMock()
    mock_table.schema = [mock_field]

    with patch('app.client') as mock_bq:
        mock_bq.get_table.return_value = mock_table
        response = client.get('/schema/WHOWNER/BT_CCARD_CADASTRAL')
        assert response.status_code == 200
        data = response.get_json()
        assert 'fields' in data
        assert data['fields'][0]['name'] == 'CCARD_ACCOUNT_ID'


def test_summary_sem_body(client):
    response = client.post('/summary', json={})
    assert response.status_code == 400
    assert 'error' in response.get_json()


def test_summary_com_table(client):
    mock_field = MagicMock()
    mock_field.name = 'CCARD_ACCOUNT_ID'

    mock_table = MagicMock()
    mock_table.schema = [mock_field]

    mock_row = MagicMock()
    mock_row.__iter__ = MagicMock(return_value=iter([
        ('column_name', 'CCARD_ACCOUNT_ID'),
        ('total_rows', 100),
        ('null_count', 5),
        ('distinct_count', 95),
    ]))
    mock_row.keys.return_value = ['column_name', 'total_rows', 'null_count', 'distinct_count']

    with patch('app.client') as mock_bq:
        mock_bq.get_table.return_value = mock_table
        mock_job = MagicMock()
        mock_job.result.return_value = [{'column_name': 'CCARD_ACCOUNT_ID', 'total_rows': 100, 'null_count': 5, 'distinct_count': 95}]
        mock_bq.query.return_value = mock_job

        response = client.post('/summary', json={'table': 'WHOWNER.BT_CCARD_CADASTRAL'})
        assert response.status_code == 200
        data = response.get_json()
        assert 'columns' in data


def test_query_sem_body(client):
    response = client.post('/query', json={})
    assert response.status_code == 400
    assert 'error' in response.get_json()


def test_query_com_sql(client):
    with patch('app.client') as mock_bq:
        mock_job = MagicMock()
        mock_job.result.return_value = [{'col': 1}]
        mock_bq.query.return_value = mock_job

        response = client.post('/query', json={'sql': 'SELECT 1 AS col'})
        assert response.status_code == 200
        data = response.get_json()
        assert 'rows' in data
        assert 'total' in data
