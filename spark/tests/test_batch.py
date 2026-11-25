from __future__ import annotations

import json
from datetime import date, datetime

import backfill_job
import batch_views_job
from conftest import fixture_json, frame, kafka_stream, write_kafka_batch
from test_opensky import vector
from travelradar import config, sinks
from travelradar.sources import digitraffic_trains, opensky, weather


def test_backfill_uses_same_transformation_as_stream(spark, actual):
    """FT-BF-01: History backfill and live stream produce identical rows
    Objective: Lambda requires batch and speed layers to apply the same logic; the rail backfill must give the same stop events as the stream for the same train.
    Steps: Turn train IC 1 into rows (a) with the backfill path (rail_day_frame + rail_cleaned) and (b) with the streaming parse.
    Expected: identical sets of (train_id, stop_seq, station, delay_min, is_actual)."""
    trains = fixture_json("dt_train_1.json")
    key = lambda r: (r["train_id"], r["stop_seq"], r["station"], r["delay_min"], r["is_actual"])
    batch = {key(r) for r in backfill_job.rail_cleaned(backfill_job.rail_day_frame(spark, trains)).collect()}
    stream = {key(r) for r in digitraffic_trains.parse(frame(spark, [json.dumps(t) for t in trains])).collect()}
    assert batch == stream and len(batch) == 24
    actual(f"{len(batch)} identical stop events")


def test_weather_archive_converted_to_live_shape(spark, actual):
    """FT-BF-02: Historical weather is converted to the live record shape
    Objective: Hourly archive arrays must become rows of the same weather_observations schema as the live feed.
    Steps: Convert an archive response with 2 cities x 3 hours via weather_values and parse with weather.parse.
    Expected: 6 rows, cities helsinki and oslo, hourly event times, values taken from the right array index."""
    def city(lat, lon, temps):
        return {"latitude": lat, "longitude": lon, "hourly": {
            "time": ["2025-01-01T00:00", "2025-01-01T01:00", "2025-01-01T02:00"],
            "temperature_2m": temps, "precipitation": [0, 0.1, 0], "wind_speed_10m": [5, 6, 7]}}
    response = [city(60.17, 24.94, [-5.0, -6.0, -7.0]), city(59.91, 10.75, [1.0, 2.0, 3.0])]
    out = weather.parse(frame(spark, backfill_job.weather_values(response))).collect()
    got = sorted((r["location_id"], r["event_time"].hour, r["temperature_c"]) for r in out)
    assert got == [("helsinki", 0, -5.0), ("helsinki", 1, -6.0), ("helsinki", 2, -7.0),
                   ("oslo", 0, 1.0), ("oslo", 1, 2.0), ("oslo", 2, 3.0)]
    actual(f"{len(got)} rows: {got[:2]} ...")


def test_backfill_day_is_idempotent(spark, hdfs_root, actual):
    """FT-BF-03: Re-running a backfill day replaces it instead of duplicating it
    Objective: Failed downloads are fixed by re-running a date range; this must not duplicate days already loaded.
    Steps: write_day for 2026-11-24 twice with the same train.
    Expected: 24 stop events in batch/train_stop_events_history after both runs; 1 raw record."""
    trains = fixture_json("dt_train_1.json")
    for _ in range(2):
        f = backfill_job.rail_day_frame(spark, trains)
        backfill_job.write_day(f, backfill_job.rail_cleaned(f), "digitraffic_trains_archive",
                               "train_stop_events_history", date(2026, 11, 24))
    cleaned = spark.read.parquet(f"{config.BATCH_ROOT}/train_stop_events_history").count()
    raw = spark.read.parquet(f"{config.BATCH_ROOT}/raw/source=digitraffic_trains_archive").count()
    assert (cleaned, raw) == (24, 1)
    actual(f"after 2 runs: {cleaned} stop events, {raw} raw record")


def _archive_opensky(spark, tmp_path):
    src = tmp_path / "kafka"
    t = datetime(2026, 11, 22, 1, 20)
    write_kafka_batch(src, "opensky", [vector(icao=f"00000{i}") for i in range(4)] + ["junk"], t)
    for start in (sinks.start_raw_archive, sinks.start_cleaned_archive):
        start(spark, opensky.SOURCE, "availableNow", kafka_stream(spark, src)).awaitTermination()


def test_volume_view(spark, hdfs_root, tmp_path, actual):
    """FT-BV-01: Source volume view counts records per topic and day
    Objective: The batch view behind the data set description (records per day, bytes, rate) must count the raw archive correctly.
    Steps: Archive 5 OpenSky records (4 valid, 1 junk) on 2026-11-22 and compute source_volume_daily.
    Expected: one row for topic opensky, dt 2026-11-22, records = 5, bytes = total length of the values."""
    _archive_opensky(spark, tmp_path)
    rows = batch_views_job.source_volume(spark, ["2026-11-22"]).collect()
    assert len(rows) == 1
    r = rows[0]
    assert (r["topic"], r["dt"], r["records"]) == ("opensky", "2026-11-22", 5)
    assert r["bytes"] == sum(len(vector(icao=f"00000{i}")) for i in range(4)) + len("junk")
    actual(r.asDict())


def test_consistency_view_stream_equals_batch(spark, hdfs_root, tmp_path, actual):
    """FT-BV-02: Batch layer recomputes the speed layer's output from raw data
    Objective: Demonstrates reprocessing of historical data: re-parsing the raw archive in batch gives the same cleaned rows the streaming job wrote.
    Steps: Archive 5 records through the streaming queries (received 2026-11-22, position fixes of 2026-11-21: raw and cleaned fall on different days), then compute consistency_daily for 2026-11-21.
    Expected: flight_positions on 2026-11-21: stream_rows = batch_rows = 4, diff_pct = 0."""
    _archive_opensky(spark, tmp_path)
    rows = batch_views_job.consistency(spark, ["2026-11-21"]).collect()
    r = next(x for x in rows if x["table"] == "flight_positions")
    assert (r["stream_rows"], r["batch_rows"], r["diff_pct"]) == (4, 4, 0.0)
    actual(r.asDict())


def test_rail_delay_view(spark, hdfs_root, actual):
    """FT-BV-03: Rail delay statistics from measured arrivals
    Objective: Check the batch view used for EDA and the ML baselines.
    Steps: Load train IC 1 through the backfill, compute rail_delay_daily for 2026-11-24.
    Expected: one row (Long-distance) whose arrivals and late share equal a direct count over the measured, non-cancelled arrivals of the train."""
    trains = fixture_json("dt_train_1.json")
    f = backfill_job.rail_day_frame(spark, trains)
    backfill_job.write_day(f, backfill_job.rail_cleaned(f), "digitraffic_trains_archive",
                           "train_stop_events_history", date(2026, 11, 24))
    rows = batch_views_job.rail_delay(spark, ["2026-11-24"]).collect()
    arrivals = [r for r in trains[0]["timeTableRows"]
                if r.get("commercialStop") and r["type"] == "ARRIVAL" and r.get("actualTime")
                and not r["cancelled"]]
    assert len(rows) == 1 and rows[0]["train_category"] == "Long-distance"
    assert rows[0]["arrivals"] == len(arrivals)
    late = sum(r["differenceInMinutes"] >= 3 for r in arrivals) / len(arrivals)
    assert abs(rows[0]["late_share"] - round(late, 4)) < 1e-9
    actual(rows[0].asDict())
