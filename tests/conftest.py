from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from api import create_app
from models import Base, Station, WeatherRaw, WeatherStats

# Pytest fixtures to set up an memory test app and client with seeded data
@pytest.fixture()
def app():
    app = create_app("sqlite:///:memory:")
    engine = app.config["ENGINE"]
    SessionLocal = sessionmaker(bind=engine)
    with app.app_context():
        Base.metadata.create_all(engine)
        session = SessionLocal()
        station = Station(id="ST001")
        session.add(station)
        session.add_all([
            WeatherRaw(station_id="ST001", date=date(2020, 1, 1), tmax=20, tmin=5, precipitation=10),
            WeatherRaw(station_id="ST001", date=date(2020, 1, 2), tmax=25, tmin=10, precipitation=0),
        ])
        session.add(WeatherStats(station_id="ST001", year=2020, avg_tmax=2.0, avg_tmin=1.0, total_precip=0.1))
        session.commit()
        yield app
        session.close()


@pytest.fixture()
def client(app):
    return app.test_client()
