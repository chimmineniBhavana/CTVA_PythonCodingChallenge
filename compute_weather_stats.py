import logging
from sqlalchemy import create_engine, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session
import click

from models import Base, WeatherRaw, WeatherStats


@click.command()
@click.option(
    '--db-url',
    default='postgresql://avnadmin:AVNS_WDdFU6_4K9qT5Nk0-iK@support-harshp-c41f.f.aivencloud.com:13002/weather?sslmode=require',
    show_default=True,
)
@click.option('--verbose', is_flag=True, help='Enable debug logging')
def main(db_url: str, verbose: bool) -> None:
    """Compute yearly weather statistics per station."""
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')
    engine = create_engine(db_url)
    # Ensure required tables exist
    Base.metadata.create_all(engine)
    dialect = engine.dialect.name
    insert_fn = pg_insert if dialect == 'postgresql' else sqlite_insert

    with Session(engine) as session:
        subq = (
            session.query(
                WeatherRaw.station_id.label('station_id'),
                func.extract('year', WeatherRaw.date).label('year'),
                (func.avg(WeatherRaw.tmax) / 10).label('avg_tmax'),
                (func.avg(WeatherRaw.tmin) / 10).label('avg_tmin'),
                (func.sum(WeatherRaw.precipitation) / 100).label('total_precip'),
            )
            .group_by(WeatherRaw.station_id, func.extract('year', WeatherRaw.date))
            .subquery()
        )

        stmt = insert_fn(WeatherStats).from_select(
            ('station_id', 'year', 'avg_tmax', 'avg_tmin', 'total_precip'), subq
        )
        if dialect == 'postgresql':
            stmt = stmt.on_conflict_do_update(
                index_elements=['station_id', 'year'],
                set_={
                    'avg_tmax': stmt.excluded.avg_tmax,
                    'avg_tmin': stmt.excluded.avg_tmin,
                    'total_precip': stmt.excluded.total_precip,
                },
            )
        else:
            stmt = stmt.prefix_with('OR REPLACE')

        result = session.execute(stmt)
        session.commit()
        logging.info(
            'Computed statistics for %d station-year combinations', result.rowcount
        )


if __name__ == '__main__':
    main()
