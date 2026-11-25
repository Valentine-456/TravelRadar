# Nordic Travel Radar Dashboard

*Multimodal travel intelligence with streaming delay prediction*

```
 sources                 acquisition      buffer        processing (Spark)                 storage / serving
 ─────────────────────   ─────────────    ─────────     ──────────────────────────────     ─────────────────────────
 OpenSky (flights)  ─┐                                  speed layer:  hot_planes/ships ──▶ Redis ──▶ FastAPI ──▶ map
 AISStream (ships)   │                                  (10 s)
 Digitraffic MQTT    ├─▶ NiFi ──────────▶ Kafka ──────▶ archive:  raw_<source>      ──▶ HDFS raw/      ┐
   (trains, GPS)     │   (6 flows)        (6 topics,    (10 min)  cleaned_<table>   ──▶ HDFS cleaned/  ├─▶ Hive
 Entur (NO vehicles) │                     72 h)                                                         │  (SQL)
 Open-Meteo weather ─┘                                  batch layer: backfill_job ────▶ HDFS batch/    │
 Digitraffic / Open-Meteo archives ─────────────────────────────────  batch_views_job ─▶ HDFS views/  ┘
```

Lambda architecture: Kafka feeds a speed layer (Redis, map) and an immutable master dataset on HDFS
(raw records). Batch jobs load history and recompute views from the master dataset with the same
parsers as the stream; `consistency_daily` shows that both give the same rows.

| Folder | Content |
| --- | --- |
| [`nifi/`](nifi/README.md) | acquisition flows (JSON) and `deploy.py` |
| [`kafka/`](kafka/topics.txt) | topic list, created by `kafka-init` |
| [`spark/`](spark/README.md) | streaming pipeline, backfills, batch views, unit tests |
| [`hdfs/`](hdfs/README.md), [`hive/`](hive/README.md) | HDFS config and layout, Hive tables |
| [`backend/`](backend/README.md), [`frontend/`](frontend/README.md) | API and map |
| [`tests/`](tests/README.md) | NiFi flow checks, integration tests, test report |

## Setup

Requirements: Docker Desktop with 8 GB memory and 15 GB free disk, [uv](https://docs.astral.sh/uv/),
Node.js, Python 3.9+.

### 1. Secrets

```bash
cp nifi/.env.example nifi/.env         # OpenSky client id/secret, AISStream API key
cp backend/.env.example backend/.env   # Mapbox token
```

Digitraffic, Entur and Open-Meteo need no key. Without `nifi/.env` the OpenSky and AIS flows stay
stopped; everything else works.

### 2. Infrastructure

```bash
docker compose up -d --build
```

Starts Kafka, NiFi, Redis, HDFS (namenode + datanode), Hive and Spark. One-shot services create the
Kafka topics (`kafka-init`) and the HDFS folders (`hdfs-init`); Hive creates its tables on start.

| UI | URL |
| --- | --- |
| NiFi | https://localhost:8443/nifi (`admin` / `travelradar123`) |
| Spark master | http://localhost:8080 |
| HDFS namenode | http://localhost:9870 |
| HiveServer2 | http://localhost:10002 (JDBC `jdbc:hive2://localhost:10000/travelradar`) |

### 3. NiFi flows

```bash
python3 nifi/deploy.py            # import all flows, load keys from nifi/.env, start them
python3 nifi/deploy.py --status   # state of every flow
```

### 4. Spark

```bash
cd spark
./submit.sh -d pipeline_job.py                       # streaming pipeline (all sources), runs forever
./submit.sh backfill_job.py rail --days 60           # Finnish rail history
./submit.sh backfill_job.py weather --start 2025-01-01 --end 2026-11-24
./submit.sh batch_views_job.py --all                 # batch views (volume, quality, consistency, delays)
```

Logs in `spark/logs/`. Stop a background job: http://localhost:8080 → **(kill)**.

### 5. Backend + frontend

```bash
cd backend
(cd ../frontend && npm ci && npm run build) && rm -rf static && cp -r ../frontend/dist static
uv sync
uv run fastapi run
```

Open http://localhost:8000. For frontend development: `uv run fastapi dev` in `backend/` and
`npm run dev` in `frontend/`.

### 6. Query the data

```bash
docker compose exec hive beeline -u jdbc:hive2://localhost:10000/travelradar -n hive \
  -e "SELECT train_category, AVG(delay_min) FROM train_stop_events WHERE is_actual GROUP BY train_category;"
```

## Tests

```bash
./run_tests.sh                 # Spark unit, backend API, NiFi flow checks (no stack needed)
./run_tests.sh --integration   # + integration tests against the running stack
```

Every run writes `tests/reports/test_cases.md` / `.csv`: one row per test case with objective, steps,
expected and actual result. See [`tests/README.md`](tests/README.md).
