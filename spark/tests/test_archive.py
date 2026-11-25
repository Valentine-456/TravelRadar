from __future__ import annotations

from datetime import datetime

from conftest import kafka_stream, write_kafka_batch
from test_opensky import vector
from travelradar import config, sinks
from travelradar.sources import opensky

T0 = datetime(2026, 11, 22, 1, 20, 0)


def run(query):
    query.awaitTermination()
    return query


def test_raw_archive_keeps_every_record(spark, hdfs_root, tmp_path, actual):
    """FT-AR-01: Raw archive stores every Kafka record unchanged
    Objective: The raw layer is the master dataset; it must contain every record exactly as received, including invalid ones.
    Steps: 1) Stream 3 records (2 valid vectors, 1 malformed) through start_raw_archive. 2) Read raw/events/topic=opensky.
    Expected: 3 rows; value byte-identical to the input; partition/offset kept; partition folder dt=2026-11-22."""
    src = tmp_path / "kafka"
    values = [vector(icao="000001"), vector(icao="000002"), "garbage"]
    write_kafka_batch(src, "opensky", values, T0)
    run(sinks.start_raw_archive(spark, opensky.SOURCE, "availableNow", kafka_stream(spark, src)))
    raw = spark.read.parquet(sinks.raw_path("opensky"))
    assert sorted(r["value"] for r in raw.collect()) == sorted(values)
    assert {r["dt"] for r in raw.collect()} == {"2026-11-22"}
    assert sorted(r["offset"] for r in raw.collect()) == [0, 1, 2]
    assert (hdfs_root / "raw/events/topic=opensky/dt=2026-11-22").is_dir()
    actual(f"{raw.count()} raw rows, columns {raw.columns}")


def test_cleaned_archive_deduplicates(spark, hdfs_root, tmp_path, actual):
    """FT-AR-02: Cleaned archive drops repeated and invalid records
    Objective: OpenSky repeats the last vector of an aircraft until it gets a new fix; the cleaned table keeps each fix once.
    Steps: Stream poll 1 (aircraft A fix t1, B fix t1, one malformed record) and poll 2 two minutes later (A fix t1 again, A fix t2) through start_cleaned_archive.
    Expected: 3 rows (A t1, A t2, B t1); the malformed record is dropped."""
    src = tmp_path / "kafka"
    t1, t2 = 1795281400, 1795281520
    write_kafka_batch(src, "opensky", [vector(icao="00000a", tpos=t1), vector(icao="00000b", tpos=t1),
                                       "[1,2]"], T0)
    write_kafka_batch(src, "opensky", [vector(icao="00000a", tpos=t1), vector(icao="00000a", tpos=t2)],
                      datetime(2026, 11, 22, 1, 22), first_offset=3)
    run(sinks.start_cleaned_archive(spark, opensky.SOURCE, "availableNow", kafka_stream(spark, src)))
    out = spark.read.parquet(config.cleaned_path("flight_positions")).collect()
    keys = sorted((r["icao24"], int(r["event_time"].timestamp())) for r in out)
    assert keys == [("00000a", t1), ("00000a", t2), ("00000b", t1)]
    actual(f"{len(out)} rows from 5 records: {keys}")


def test_restart_is_exactly_once(spark, hdfs_root, tmp_path, actual):
    """FT-AR-03: Restarting the pipeline does not duplicate data
    Objective: After a crash or restart the job resumes from its HDFS checkpoint; nothing may be written twice (this was a bug in the first HDFS version).
    Steps: 1) Archive 2 records. 2) Start the query again with the same checkpoint and no new data. 3) Add 1 record and start again.
    Expected: row counts 2, then 2, then 3, for both the raw and the cleaned archive."""
    src = tmp_path / "kafka"
    counts = []

    def archive():
        run(sinks.start_raw_archive(spark, opensky.SOURCE, "availableNow", kafka_stream(spark, src)))
        run(sinks.start_cleaned_archive(spark, opensky.SOURCE, "availableNow", kafka_stream(spark, src)))
        counts.append((spark.read.parquet(sinks.raw_path("opensky")).count(),
                       spark.read.parquet(config.cleaned_path("flight_positions")).count()))

    write_kafka_batch(src, "opensky", [vector(icao="000001"), vector(icao="000002")], T0)
    archive()
    archive()
    write_kafka_batch(src, "opensky", [vector(icao="000003")], T0, first_offset=2)
    archive()
    assert counts == [(2, 2), (2, 2), (3, 3)]
    actual(f"(raw, cleaned) after each run: {counts}")


def test_cleaned_schema_matches_hive_ddl(spark, hdfs_root, tmp_path, actual):
    """FT-AR-04: Cleaned Parquet schema matches the Hive table definition
    Objective: Hive external tables read the Parquet files by column name; a renamed column would silently read as NULL.
    Steps: Archive one record per source (where fixtures exist) and compare the Parquet columns with hive/tables.sql (copied into the test via the shared DDL file).
    Expected: identical column names and order for flight_positions (other tables: tests/integration)."""
    src = tmp_path / "kafka"
    write_kafka_batch(src, "opensky", [vector()], T0)
    run(sinks.start_cleaned_archive(spark, opensky.SOURCE, "availableNow", kafka_stream(spark, src)))
    cols = spark.read.parquet(config.cleaned_path("flight_positions")).columns
    expected = ["icao24", "callsign", "origin_country", "time_position", "last_contact", "event_time",
                "longitude", "latitude", "baro_altitude", "geo_altitude", "altitude", "on_ground",
                "velocity", "true_track", "vertical_rate", "squawk", "position_source", "ingest_time", "dt"]
    assert cols == expected
    actual(f"columns: {cols}")
