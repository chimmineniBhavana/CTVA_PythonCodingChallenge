from __future__ import annotations

from datetime import datetime
from flask import Flask, request, g
from flask_restx import Api, Resource, fields
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Base, WeatherRaw, WeatherStats
import ingest_weather
import compute_weather_stats


def create_app(db_url: str | None = None) -> Flask:
    """Application factory to create and configure the Flask app"""
    app = Flask(__name__)
    app.config.setdefault(
        "DATABASE_URL",
        db_url
        or "postgresql://avnadmin:AVNS_WDdFU6_4K9qT5Nk0-iK@support-harshp-c41f.f.aivencloud.com:13002/weather?sslmode=require",
    )
    engine = create_engine(app.config["DATABASE_URL"])
    SessionLocal = sessionmaker(bind=engine)

    app.config["ENGINE"] = engine
    app.config["SessionLocal"] = SessionLocal

    api = Api(app, version="1.0", title="Weather API", description="Weather data API", prefix="/api", doc="/api/swagger")

    weather_model = api.model(
        "WeatherRaw",
        {
            "station_id": fields.String,
            "date": fields.String,
            "tmax": fields.Float,
            "tmin": fields.Float,
            "precipitation": fields.Float,
        },
    )

    stats_model = api.model(
        "WeatherStats",
        {
            "station_id": fields.String,
            "year": fields.Integer,
            "avg_tmax": fields.Float,
            "avg_tmin": fields.Float,
            "total_precip": fields.Float,
        },
    )

    def get_db():
        if "db" not in g:
            g.db = SessionLocal()
        return g.db

    @app.teardown_appcontext
    def shutdown_session(exc: Exception | None = None):
        db = g.pop("db", None)
        if db is not None:
            db.close()

    @api.route("/weather")
    class WeatherList(Resource):
        @api.doc(params={
            "station_id": "Filter by station id",
            "date": "Filter by date YYYY-MM-DD",
            "page": "Page number",
            "per_page": "Items per page",
        })
        @api.marshal_with(api.model("WeatherList", {
            "items": fields.List(fields.Nested(weather_model)),
            "page": fields.Integer,
            "per_page": fields.Integer,
            "total": fields.Integer,
        }))
        def get(self):  # type: ignore[override]
            session = get_db()
            query = session.query(WeatherRaw)
            station_id = request.args.get("station_id")
            if station_id:
                query = query.filter(WeatherRaw.station_id == station_id)
            date_str = request.args.get("date")
            if date_str:
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d").date()
                except ValueError:
                    api.abort(400, "Invalid date format, expected YYYY-MM-DD")
                query = query.filter(WeatherRaw.date == dt)
            total = query.count()
            try:
                page = int(request.args.get("page", 1))
                per_page = min(int(request.args.get("per_page", 50)), 100)
            except (ValueError, TypeError):
                api.abort(400, "page and per_page must be valid integers.")
            records = (
                query.order_by(WeatherRaw.date)
                .offset((page - 1) * per_page)
                .limit(per_page)
                .all()
            )
            return {
                "items": [
                    {
                        "station_id": r.station_id,
                        "date": r.date.isoformat(),
                        "tmax": r.tmax,
                        "tmin": r.tmin,
                        "precipitation": r.precipitation,
                    }
                    for r in records
                ],
                "page": page,
                "per_page": per_page,
                "total": total,
            }

    @api.route("/weather/stats")
    class WeatherStatsList(Resource):
        @api.doc(params={
            "station_id": "Filter by station id",
            "year": "Filter by year",
            "page": "Page number",
            "per_page": "Items per page",
        })
        @api.marshal_with(api.model("WeatherStatsList", {
            "items": fields.List(fields.Nested(stats_model)),
            "page": fields.Integer,
            "per_page": fields.Integer,
            "total": fields.Integer,
        }))
        def get(self):  # type: ignore[override]
            session = get_db()
            query = session.query(WeatherStats)
            station_id = request.args.get("station_id")
            if station_id:
                query = query.filter(WeatherStats.station_id == station_id)
            year = request.args.get("year")
            if year:
                if not year.isdigit():
                    api.abort(400, "year must be numeric")
                query = query.filter(WeatherStats.year == int(year))
            total = query.count()
            try:
                page = int(request.args.get("page", 1))
                per_page = min(int(request.args.get("per_page", 50)), 100)
            except (ValueError, TypeError):
                api.abort(400, "page and per_page must be valid integers.")
            records = (
                query.order_by(WeatherStats.year)
                .offset((page - 1) * per_page)
                .limit(per_page)
                .all()
            )
            return {
                "items": [
                    {
                        "station_id": r.station_id,
                        "year": r.year,
                        "avg_tmax": r.avg_tmax,
                        "avg_tmin": r.avg_tmin,
                        "total_precip": r.total_precip,
                    }
                    for r in records
                ],
                "page": page,
                "per_page": per_page,
                "total": total,
            }

    return app


if __name__ == "__main__":
    app = create_app()
    Base.metadata.create_all(app.config["ENGINE"])
    db_url = app.config["DATABASE_URL"]
    # Run ingestion and statistics computation before starting the API
    ingest_weather.main.callback(
        db_url=db_url,
        data_dir="wx_data",
        dry_run=False,
        verbose=False,
    )
    compute_weather_stats.main.callback(db_url=db_url, verbose=False)
    app.run(debug=True)
