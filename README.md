<!-- Setup, usage, and testing guide for the Weather API serving raw and aggregated weather data -->

# Weather API

This project exposes raw and aggregated weather data via a simple REST API built with Flask and Flask-RESTX.

## Setup

1. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Set the `DATABASE_URL` environment variable if you need a custom connection string. The application defaults to `postgresql://avnadmin:AVNS_WDdFU6_4K9qT5Nk0-iK@support-harshp-c41f.f.aivencloud.com:13002/weather?sslmode=require`:
   ```bash
   export DATABASE_URL=postgresql://avnadmin:AVNS_WDdFU6_4K9qT5Nk0-iK@support-harshp-c41f.f.aivencloud.com:13002/weather?sslmode=require
   ```
3. Create the database tables:
   ```bash
   python api.py  # tables will be created if missing
   ```
4. To load the provided weather data, run:
   ```bash
   python ingest_weather.py
   ```
   The script tracks processed files so rerunning is fast.

## Running

Run the API locally with:
```bash
python api.py
```
The server listens on `http://localhost:5000/`.

Swagger/OpenAPI documentation is available at `http://localhost:5000/api/swagger`.

### Query Parameters

Both `/api/weather` and `/api/weather/stats` accept:
- `station_id` – filter by station id
- `page` – page number (default 1)
- `per_page` – items per page (default 50, max 100)

`/api/weather` additionally accepts `date` (`YYYY-MM-DD`) and `/api/weather/stats` accepts `year`.

## Testing

Run unit tests with:
```bash
PYTHONPATH=. pytest -q
```

