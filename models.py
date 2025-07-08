"""SQLAlchemy ORM models for weather data."""

from sqlalchemy import (
    Column,
    Date,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from datetime import datetime
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Station(Base):
    """Weather station metadata."""

    __tablename__ = "station"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    state = Column(String, nullable=True)

    weather_records = relationship("WeatherRaw", back_populates="station")
    yearly_stats = relationship("WeatherStats", back_populates="station")


class WeatherRaw(Base):
    """Daily raw weather measurements."""

    __tablename__ = "weather_raw"
    __table_args__ = (
        UniqueConstraint("station_id", "date", name="uix_weather_raw_station_date"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(String, ForeignKey("station.id"), nullable=False)
    date = Column(Date, nullable=False)
    tmax = Column(Integer, nullable=True)
    tmin = Column(Integer, nullable=True)
    precipitation = Column(Integer, nullable=True)

    station = relationship("Station", back_populates="weather_records")


class WeatherStats(Base):
    """Yearly aggregated weather statistics."""

    __tablename__ = "weather_stats"
    __table_args__ = (
        UniqueConstraint("station_id", "year", name="uix_weather_stats_station_year"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(String, ForeignKey("station.id"), nullable=False)
    year = Column(Integer, nullable=False)
    avg_tmax = Column(Float, nullable=True)
    avg_tmin = Column(Float, nullable=True)
    total_precip = Column(Float, nullable=True)

    station = relationship("Station", back_populates="yearly_stats")


class CropYield(Base):
    """Annual crop yield measurements."""

    __tablename__ = "crop_yield"

    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False, unique=True)
    yield_value = Column(Integer, nullable=True)


class IngestedFile(Base):
    """Track processed weather files to avoid reloading."""

    __tablename__ = "ingested_file"

    file_name = Column(String, primary_key=True)
    ingested_at = Column(Date, default=datetime.utcnow)
