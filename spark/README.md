# Spark

PySpark jobs for TravelRadar. This folder is mounted into the Spark containers at `/opt/spark/apps`.

- Master UI: http://localhost:8080, worker UI: http://localhost:8081
- Custom image (`Dockerfile`): adds `redis`, `pytest`, `fakeredis`. After changing it:
  `docker compose up -d --build spark-master spark-worker`

## Jobs

| Job | Kind | What it does |
| --- | --- | --- |
| `pipeline_job.py` | streaming | every source: Kafka → raw archive + cleaned archive on HDFS (10 min); OpenSky/AIS also → Redis snapshots (10 s) |
| `backfill_job.py` | batch | history: `rail` (Digitraffic archive, per day) and `weather` (Open-Meteo archive) → HDFS `batch/` |
| `batch_views_job.py` | batch | views from HDFS: `source_volume_daily`, `quality_daily`, `consistency_daily`, `rail_delay_daily` |
| `hdfs_smoke_write.py` | check | writes and reads 10 rows on HDFS |

```bash
./submit.sh -d pipeline_job.py                  # background, log in logs/pipeline_job.log
./submit.sh -d pipeline_job.py entur weather    # only some sources
./submit.sh pipeline_job.py --once              # process what is in Kafka now, then stop
./submit.sh backfill_job.py rail --days 60
./submit.sh batch_views_job.py --dt 2026-11-25
```

`submit.sh` forwards these environment variables to the driver and executors:
`TRAVELRADAR_ROOT` (HDFS root, default `hdfs://namenode:9000/travelradar`), `ARCHIVE_TRIGGER`
(e.g. `600` seconds or `availableNow`), `HOT_TRIGGER`, `TOPIC_PREFIX`, `REDIS_PREFIX` (the last two
isolate the integration test). `DRIVER_MEMORY`, `EXECUTOR_MEMORY` and `CORES` size the application.

## Code layout (`jobs/travelradar/`)

| Module | Role |
| --- | --- |
| `sources/<source>.py` | one per source: `parse()` (Kafka record → typed, validated rows), dedup keys, quality rules |
| `sinks.py` | raw and cleaned archive queries (Parquet file sink, exactly once, checkpoints on HDFS) |
| `hot.py` | speed layer: plane/ship snapshots in Redis for the map |
| `quality.py` | data-quality checks driven by each source's `quality_rules` |
| `hive_ddl.py` | generates `hive/tables.sql` from the parsers' schemas (`./gen_hive_ddl.sh`) |
| `config.py`, `session.py`, `common.py` | settings, Spark session, shared helpers |

The streaming pipeline, the backfills and the tests all call the same `parse()` functions.

## Sources → tables

| Source | Topic | Cleaned table | Dedup key |
| --- | --- | --- | --- |
| OpenSky | `opensky` | `flight_positions` | icao24, event_time |
| AISStream | `ais` | `ship_positions` | mmsi, event_time |
| Digitraffic trains (MQTT) | `digitraffic-trains` | `train_stop_events` (one row per commercial stop) | train_id, stop_seq + state |
| Digitraffic GPS (MQTT) | `digitraffic-locations` | `train_positions` | train_id, event_time |
| Entur | `entur-vehicles` | `vehicle_positions` | vehicle_id, event_time |
| Open-Meteo | `weather` | `weather_observations` | location_id, event_time |

HDFS layout: `raw/events/topic=<topic>/dt=<ingest day>`, `cleaned/<table>/dt=<event day>`,
`batch/<table>/dt=...`, `views/<view>/dt=...`, `checkpoints/<query>`.

## Redis (speed layer)

| Key | Type | Content |
| --- | --- | --- |
| `planes:snaps` / `ships:snaps` | sorted set | snapshot times (Unix seconds) |
| `planes:snap:<minute>` | hash | `icao24` → plane JSON, expires 24 h after the minute |
| `ships:snap:<5-minute bucket>` | hash | `mmsi` → ship JSON, expires after 24 h |

## Tests

```bash
./test.sh                 # all unit tests in the Spark image (no stack needed)
./test.sh -k ais          # pytest arguments pass through
./gen_hive_ddl.sh         # after changing a parser: regenerate hive/tables.sql (a test checks it)
```
