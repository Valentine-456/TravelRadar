# Spark

PySpark jobs for TravelRadar. This folder is mounted into the Spark containers at `/opt/spark/apps`. Jobs live in `jobs/`.

- Master UI: http://localhost:8080
- Worker UI: http://localhost:8081

## Run a job

On the cluster (Kafka connector included):

```bash
./submit.sh example_job.py
```

Locally on your machine, without Docker (needs Java):

```bash
uv run jobs/example_job.py
```

Inside the cluster use `kafka:9092` and `redis:6379`. Locally use `localhost:9094` and `localhost:6379`.
