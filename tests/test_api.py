from datetime import date

# Tests for Weather API endpoints
def test_weather_list(client):
    resp = client.get('/api/weather')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['total'] == 2
    assert len(data['items']) == 2


def test_weather_filter_by_date(client):
    resp = client.get('/api/weather', query_string={'date': '2020-01-01'})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['total'] == 1
    assert data['items'][0]['date'] == '2020-01-01'


def test_weather_stats(client):
    resp = client.get('/api/weather/stats')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['total'] == 1
    assert data['items'][0]['year'] == 2020


def test_invalid_pagination(client):
    resp = client.get('/api/weather', query_string={'page': 'abc'})
    assert resp.status_code == 400
    assert b'page and per_page must be valid integers' in resp.data

    resp = client.get('/api/weather/stats', query_string={'per_page': 'xyz'})
    assert resp.status_code == 400
    assert b'page and per_page must be valid integers' in resp.data
