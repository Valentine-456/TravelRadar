# HDFS

Single-node HDFS (one namenode, one datanode, replication 1): the batch layer's storage. It is part
of the normal stack (`docker compose up -d`). It uses the `apache/hive:4.0.1` image, which ships Hadoop
3.3.6 and is published for amd64 and arm64, so it runs natively on Apple Silicon too.

- Namenode UI: http://localhost:9870, RPC `hdfs://namenode:9000`
- Config shared by HDFS, Hive and Spark: `conf/core-site.xml`, `conf/hdfs-site.xml`
- `start-namenode.sh` formats the namenode on first start; `init-dirs.sh` (one-shot `hdfs-init`)
  creates the folders

## Layout

```
/travelradar
├── raw/events/topic=<topic>/dt=<ingest day>/    bronze: every Kafka record unchanged (master dataset)
├── cleaned/<table>/dt=<event day>/              silver: parsed, validated, de-duplicated
├── batch/raw/source=<archive>/dt=...            raw history downloads
├── batch/<table>/dt=...                         history in the cleaned schema
├── views/<view>/dt=...                          gold: batch views
├── models/                                      MS3+
└── checkpoints/<query>/                         Spark streaming checkpoints (exactly-once)
```

All data is Parquet (snappy), written by Spark, and exposed as Hive tables (see `../hive`).

## Useful commands

```bash
docker compose exec namenode hdfs dfs -du -h /travelradar/raw/events /travelradar/cleaned
docker compose exec namenode hdfs dfsadmin -report
docker compose run --rm hdfs-init               # re-create missing folders
cd spark && ./submit.sh hdfs_smoke_write.py      # Spark → HDFS write/read check
```

`docker compose down -v` deletes the HDFS volumes, i.e. all history.
