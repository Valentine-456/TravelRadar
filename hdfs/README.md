# HDFS (warehouse profile)

Single-node HDFS for TravelRadar’s durable Parquet store (Lambda batch/history leg). Redis remains the hot API cache.

## Start

From the repo root (needs ~1.5 GB extra RAM):

```bash
docker compose --profile warehouse up -d
# hdfs-init runs once and creates /travelradar/...
```

NameNode UI: http://localhost:9870

## Layout

```
/travelradar/raw/…
/travelradar/cleaned/flights|ships|vehicles|weather/   # partitioned Parquet (dt, hour)
/travelradar/batch/…
/travelradar/models/…
/travelradar/checkpoints/opensky|aisstream
```

## Smoke test (Spark → HDFS → read-back)

With Spark + warehouse up:

```bash
cd spark
./submit.sh hdfs_smoke_write.py
```

Streaming jobs always try to append Parquet under `cleaned/flights` and `cleaned/ships`. Redis still works if HDFS is down. For durable Spark checkpoints on HDFS:

```bash
docker compose exec -e USE_HDFS_CHECKPOINTS=1 spark-master ...
# or export before ./submit.sh once submit.sh forwards the env
```

Inspect:

```bash
docker compose exec namenode hdfs dfs -ls -R /travelradar/cleaned/_smoke
```

## Re-init directories

```bash
docker compose run --rm hdfs-init
# or
docker compose exec namenode bash /init-dirs.sh   # if script is mounted on namenode
```

To mount the init script on the namenode for manual runs, the one-shot `hdfs-init` service already mounts it.

## Stop warehouse only

```bash
docker compose --profile warehouse stop namenode datanode
```

`docker compose down -v` deletes HDFS volumes (all history).
