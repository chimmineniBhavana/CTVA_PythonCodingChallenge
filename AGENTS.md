# agents.md

This document defines the high-level “agents” (logical components) in our weather data pipeline.  
Each agent is designed for clear responsibility, testability, and ease of handoff.

---

## 1. Data Modeling Agent

**Purpose:**  
Define and migrate the database schema for both raw weather records and aggregated statistics.

**Responsibilities:**  
- Declare ORM models or SQL DDL for:
  - `station` (id, name, latitude, longitude, state)
  - `weather_raw` (station_id, date, tmax, tmin, precipitation)
  - `weather_stats` (station_id, year, avg_tmax, avg_tmin, total_precip)

**Tech & Patterns:**  
- **PostgreSQL**  
- **SQLAlchemy ORM** with Alembic for migrations  
- Naming conventions + unique constraints on `(station_id, date)` and `(station_id, year)`

---

## 2. Ingestion Agent

**Purpose:**  
Parse raw text files and load data into `weather_raw`, upserting to avoid duplicates.

**Responsibilities:**  
- Walk `wx_data/` directory, parse lines (`YYYYMMDD\tint\tint\tint`), normalize units.  
- Filter out `-9999` as `NULL`.  
- Upsert into DB.  
- Emit logs: start/end timestamps, file names, rows processed, errors.

**Tech & Patterns:**  
- Python script with **Click** for CLI interface (`--data-dir`, `--dry-run`, `--verbose`)  
- SQLAlchemy bulk upsert pattern or Postgres `INSERT … ON CONFLICT`  
- **Logging** via Python’s `logging` module

---

## 3. Analysis Agent

**Purpose:**  
Compute yearly stats per station and store them in `weather_stats`.

**Responsibilities:**  
- Query `weather_raw` grouped by `(station_id, year)`.  
- Calculate:
  - `avg_tmax` = AVG(tmax) / 10
  - `avg_tmin` = AVG(tmin) / 10
  - `total_precip` = SUM(precip) / 100
- Handle NULLs gracefully.  
- Upsert results into `weather_stats`.  

**Tech & Patterns:**  
- Python ETL script or SQL stored procedure.  
- Tests against a small SQLite fixture.  
- Scheduled (via cron or scheduler agent below).

---

## 4. API Agent

**Purpose:**  
Expose raw and aggregated data via JSON REST endpoints.

**Endpoints:**  
```text
GET /api/weather         # raw data
GET /api/weather/stats   # aggregated stats
