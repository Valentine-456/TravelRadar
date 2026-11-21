# Spark

PySpark jobs for TravelRadar. This folder is mounted into the Spark containers at `/opt/spark/apps`. Jobs live in `jobs/`.

- Master UI: http://localhost:8080
- Worker UI: http://localhost:8081

The Spark containers use a custom image (`Dockerfile`) that adds the `redis` Python package.
After changing it: `docker compose up -d --build spark-master spark-worker`.

## Run a job

On the cluster (Kafka connector included):

```bash
./submit.sh opensky_job.py      # foreground, Ctrl+C stops it
./submit.sh -d opensky_job.py   # background, the terminal can be closed
```

Background logs: `tail -f logs/opensky_job.log`

Stop a background job: http://localhost:8080 → **Running Applications** → **(kill)**.

Locally on your machine, without Docker (needs Java):

```bash
uv run jobs/example_job.py
```

Inside the cluster use `kafka:9092` and `redis:6379`. Locally use `localhost:9094` and `localhost:6379`.

## Jobs

**`opensky_job.py`** reads plane positions from the Kafka topic `opensky` and writes them to Redis:

| Key | Type | Content |
| --- | --- | --- |
| `planes:snaps` | sorted set | available minutes (Unix seconds) |
| `planes:snap:<minute>` | hash | `icao24` → plane JSON, expires after 24 h |
