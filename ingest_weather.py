import logging
from datetime import datetime
from pathlib import Path
from typing import Iterator, Tuple

import click
from sqlalchemy import create_engine, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from models import Base, Station, WeatherRaw, IngestedFile


def parse_line(line: str) -> Tuple[str, int | None, int | None, int | None]:
    parts = line.strip().split('\t')
    if len(parts) != 4:
        raise ValueError(f"Invalid line: {line!r}")
    date_str, tmax, tmin, prcp = parts
    def val(x: str) -> int | None:
        v = int(x)
        return None if v == -9999 else v
    return date_str, val(tmax), val(tmin), val(prcp)


def iter_records(file_path: Path) -> Iterator[dict]:
    station_id = file_path.stem
    with file_path.open('r') as f:
        for line in f:
            date_str, tmax, tmin, prcp = parse_line(line)
            yield {
                'station_id': station_id,
                'date': datetime.strptime(date_str, '%Y%m%d').date(),
                'tmax': tmax,
                'tmin': tmin,
                'precipitation': prcp,
            }


@click.command()
@click.option(
    '--db-url',
    default='postgresql://avnadmin:AVNS_WDdFU6_4K9qT5Nk0-iK@support-harshp-c41f.f.aivencloud.com:13002/weather?sslmode=require',
    show_default=True,
)
@click.option('--data-dir', default='wx_data', type=click.Path(exists=True, file_okay=False))
@click.option('--dry-run', is_flag=True, help='Parse files without writing to the database')
@click.option('--verbose', is_flag=True, help='Enable debug logging')
def main(db_url: str, data_dir: str, dry_run: bool, verbose: bool) -> None:
    """Ingest weather data from text files into the database."""
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    start = datetime.now()
    logging.info('Starting ingestion from %s', data_dir)

    engine = create_engine(
        db_url,
        executemany_mode="values",
        executemany_values_page_size=1000,
    )
    # Ensure required tables exist
    Base.metadata.create_all(engine)
    dialect = engine.dialect.name
    insert_fn = pg_insert if dialect == "postgresql" else sqlite_insert

    total = 0
    with Session(engine) as session:
        ingested = {
            name
            for name in session.execute(select(IngestedFile.file_name)).scalars()
        }

        for path in sorted(Path(data_dir).glob("*.txt")):
            if path.name in ingested:
                logging.info("Skipping %s (already ingested)", path.name)
                continue

            logging.info("Processing %s", path.name)
            stmt_station = (
                insert_fn(Station)
                .values(id=path.stem)
                .on_conflict_do_nothing(index_elements=["id"])
            )
            session.execute(stmt_station)

            records = list(iter_records(path))
            if records:
                stmt = (
                    insert_fn(WeatherRaw)
                    .on_conflict_do_nothing(index_elements=["station_id", "date"])
                )
                session.execute(stmt, records)
                total += len(records)

            stmt_file = insert_fn(IngestedFile).values(file_name=path.name)
            if dialect == "postgresql":
                stmt_file = stmt_file.on_conflict_do_nothing(index_elements=["file_name"])
            else:
                stmt_file = stmt_file.prefix_with("OR IGNORE")
            session.execute(stmt_file)

            if dry_run:
                session.rollback()
                logging.info("Dry-run mode: rolled back transaction for %s", path.name)
            else:
                try:
                    session.commit()
                except SQLAlchemyError:
                    session.rollback()
                    logging.exception("Error committing transaction for %s", path.name)
                    raise

    end = datetime.now()
    logging.info('Ingestion complete: %d records processed in %s', total, end - start)


if __name__ == '__main__':
    main()


