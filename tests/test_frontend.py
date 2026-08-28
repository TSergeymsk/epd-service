"""Тесты для frontend API."""
import pytest
from frontend import app
from unittest.mock import patch

@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

def test_index(client):
    rv = client.get('/')
    assert rv.status_code == 200
    assert 'Анализ ЕПД' in rv.data.decode('utf-8')

def test_api_addresses_empty(client):
    with patch('frontend.get_db') as mock_db:
        mock_conn = mock_db.return_value
        mock_conn.execute.return_value.fetchall.return_value = []
        rv = client.get('/api/addresses')
        assert rv.status_code == 200
        assert rv.json == []

def test_api_accounts_by_address(client):
    with patch('frontend.get_db') as mock_db:
        mock_conn = mock_db.return_value
        mock_conn.execute.return_value.fetchall.return_value = [{'id': 1, 'account_number': '123'}]
        rv = client.get('/api/accounts_by_address?address=test')
        assert rv.status_code == 200
        assert rv.json == [{'id': 1, 'account_number': '123'}]

def test_api_services(client):
    with patch('frontend.get_db') as mock_db:
        mock_conn = mock_db.return_value
        mock_conn.execute.return_value.fetchall.return_value = [{'id': 1, 'name': 'Service1', 'unit': 'ед'}]
        rv = client.get('/api/services')
        assert rv.status_code == 200
        assert rv.json == [{'id': 1, 'name': 'Service1', 'unit': 'ед'}]

def test_api_periods(client):
    with patch('frontend.get_db') as mock_db:
        mock_conn = mock_db.return_value
        mock_conn.execute.return_value.fetchall.return_value = [{'year': 2024, 'month': 1}]
        rv = client.get('/api/periods?account_ids=1')
        assert rv.status_code == 200
        assert rv.json == [{'year': 2024, 'month': 1}]

def test_api_data_missing_params(client):
    rv = client.get('/api/data')
    assert rv.status_code == 400
    assert 'Missing parameters' in rv.json['error']

def test_api_analysis_for_month_not_found(client):
    with patch('frontend.get_db') as mock_db:
        mock_conn = mock_db.return_value
        mock_conn.execute.return_value.fetchone.return_value = None
        rv = client.get('/api/analysis_for_month?address=test&year=2024&month=1')
        assert rv.status_code == 404
        assert rv.json['error'] == 'No analysis found'

def test_api_llm_details_not_found(client):
    with patch('frontend.get_db') as mock_db:
        mock_conn = mock_db.return_value
        mock_conn.execute.return_value.fetchone.return_value = None
        rv = client.get('/api/llm_details?address=test&year=2024&month=1')
        assert rv.status_code == 404
        assert rv.json['error'] == 'Period not found'

def test_api_retry_ai(client):
    with patch('frontend.get_db') as mock_db:
        mock_conn = mock_db.return_value
        mock_conn.execute.return_value.fetchone.side_effect = [
            {'id': 1},  # период
            {'id': 10}  # существующий llm_requests
        ]
        rv = client.post('/api/retry_ai', json={'address': 'test', 'year': 2024, 'month': 1})
        assert rv.status_code == 200
        assert rv.json['status'] == 'ok'