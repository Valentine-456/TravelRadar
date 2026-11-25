# HDFS Implementation Plan — Nordic Travel Radar

**Goal:** Add a durable HDFS layer so Spark can persist cleaned events as Parquet (batch leg of **Lambda**), while Redis stays the hot serving store.  
**Baseline:** Docker Compose today runs Kafka, NiFi, Redis, Spark only — no HDFS/Hive.  
**Constraint:** Docker Desktop ~8 GB RAM; stack already uses ~4+ GB idle. Keep HDFS minimal (1 NameNode + 1 DataNode, replication factor 1).

---

## 1. Role of HDFS in the architecture

```
NiFi → Kafka → Spark Structured Streaming ─┬→ Redis          (hot, ~24h, API/UI)
                                           └→ HDFS Parquet   (cold, history, ML/EDA)
                                                    │
                                              Hive (optional, MS2+) catalogs tables
```

| Store | Purpose | Retention |
|-------|---------|-----------|
| Kafka | Buffer / short replay | Days (topic retention) |
| Redis | Latest snapshots for map/API | 24 h TTL |
| **HDFS** | Durable raw + cleaned history | Weeks–months (disk-bound) |
| Hive (later) | SQL catalog over HDFS paths | Same as HDFS |

HDFS closes the PM1 claim: *“big data store on Hadoop HDFS … for raw events, history, timetables and models.”*

---

## 2. Target layout on HDFS

Use a clear medallion-style tree (simple, report-friendly):

```
/travelradar
├── raw/                          # optional: near-source dumps
│   ├── opensky/dt=YYYY-MM-DD/
│   ├── ais/dt=YYYY-MM-DD/
│   ├── digitraffic_rail/…
│   └── …
├── cleaned/                      # Spark-normalized Parquet (primary MS2 sink)
│   ├── flights/dt=YYYY-MM-DD/hour=HH/
│   ├── ships/dt=YYYY-MM-DD/hour=HH/
│   ├── vehicles/dt=…/            # future unified rail/bus schema
│   └── weather/dt=…
├── batch/                        # daily archive loads (Finnish history, GTFS, etc.)
│   ├── digitraffic_rail_archive/dt=…
│   └── timetables/operator=…/dt=…
└── models/                       # offline model artifacts (MS3+)
    └── delay_gbt/version=…
```

**Formats:** Parquet + Snappy or zstd (PM1 measured Entur: JSON 63 MB → Parquet ~2.8 MB).  
**Partitioning:** `dt` (and `hour` for high-volume streams) for prune-friendly Spark/Hive reads.

---

## 3. Docker Compose design

### 3.1 Services to add

| Service | Image (suggested) | Ports | Role |
|---------|-------------------|-------|------|
| `namenode` | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` **or** `apache/hadoop:3.3.6` (single-node config) | `9870` (UI), `8020` (RPC) | Metadata |
| `datanode` | matching DataNode image | `9864` | Block storage |

Prefer a **well-documented Hadoop 3.x Compose pair** that the team can copy and pin. Avoid multi-node YARN unless needed — Spark already has its own master/worker.

### 3.2 Volumes & network

```yaml
# sketch — to be added to docker-compose.yaml
namenode:
  environment:
    CLUSTER_NAME: travelradar
  volumes:
    - hdfs_namenode:/hadoop/dfs/name
  networks: [travelradar-net]

datanode:
  depends_on: [namenode]
  volumes:
    - hdfs_datanode:/hadoop/dfs/data
  networks: [travelradar-net]
```

- Attach both to `travelradar-net`.
- Named volumes: `hdfs_namenode`, `hdfs_datanode`.
- Replication factor **1** (`dfs.replication=1`) for single DataNode.

### 3.3 Memory budget (critical)

| Component | Suggested heap / limit |
|-----------|------------------------|
| NameNode | 512 MB–768 MB |
| DataNode | 512 MB–768 MB |
| Existing Spark worker | keep 1g (or drop to 768m if OOM) |
| NiFi / Kafka | leave as-is; shut unused UIs when demoing |

**Do this first:** measure `docker stats` before/after. If RAM is tight, run HDFS only when writing/reading history (profile in README: `compose --profile warehouse up`).

### 3.4 Optional Compose profile

```bash
docker compose --profile warehouse up -d namenode datanode
```

Keeps day-to-day map demos light; enables HDFS for acquisition/pre-processing work and for the presentation.

---

## 4. Spark ↔ HDFS integration

### 4.1 Client config on Spark image

Extend `spark/Dockerfile` (or mount a `spark/conf/core-site.xml`):

```xml
<!-- core-site.xml -->
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://namenode:8020</value>
  </property>
</configuration>
```

Ensure Spark containers can resolve `namenode` on `travelradar-net` and that the Hadoop client libs match the cluster version (Spark 3.5.x ships Hadoop 3 client — verify against chosen NameNode tag).

### 4.2 Dual-sink streaming pattern (keep Redis)

Do **not** replace Redis. Extend each streaming job (or add a sibling writer):

```
Kafka micro-batch
  → transform / quality filters
  → foreachBatch:
        1) write Redis snapshot (existing)
        2) append Parquet to hdfs://namenode:8020/travelradar/cleaned/<domain>/…
```

**Trigger:** keep 10s for Redis; for HDFS prefer **coalesce/repartition(1–4)** and write every micro-batch **or** buffer to 1–10 min partitions to avoid tiny files (align with PM1 “10 minute micro-batches”).

### 4.3 Jobs to touch

| Job | HDFS path | Partition cols |
|-----|-----------|----------------|
| `opensky_job.py` | `/travelradar/cleaned/flights/` | `dt`, `hour` |
| `aisstream_job.py` | `/travelradar/cleaned/ships/` | `dt`, `hour` |
| Future rail/bus jobs | `/travelradar/cleaned/vehicles/` | `dt`, `hour`, maybe `country` |
| Future batch loaders | `/travelradar/batch/...` | `dt` |

### 4.4 `submit.sh` updates

Add configs when submitting:

```text
--conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020
--conf spark.sql.sources.partitionOverwriteMode=dynamic
```

No extra Kafka package change required for HDFS writes.

### 4.5 Checkpoints

Move streaming checkpoints off ephemeral `/tmp` to HDFS:

```text
hdfs://namenode:8020/travelradar/checkpoints/opensky
hdfs://namenode:8020/travelradar/checkpoints/ais
```

Survives container restarts if the HDFS volume persists.

---

## 5. Bootstrap & operations

### 5.1 One-time init script (`hdfs/init-dirs.sh`)

Run after NameNode is live:

```bash
hdfs dfs -mkdir -p /travelradar/{raw,cleaned,batch,models,checkpoints}
hdfs dfs -chmod -R 777 /travelradar   # OK for single-node academic cluster
```

Wire as a short-lived `hdfs-init` service (`restart: "no"`) or document a manual `docker compose exec namenode …` step in README.

### 5.2 Smoke checks

```bash
# UI
open http://localhost:9870

# CLI
docker compose exec namenode hdfs dfs -ls /travelradar
docker compose exec spark-master \
  /opt/spark/bin/spark-shell --conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020
# scala> spark.read.parquet("hdfs://namenode:8020/travelradar/cleaned/flights").count
```

### 5.3 Day-2 ops

| Task | Approach |
|------|----------|
| Disk growth | Monitor DataNode volume; drop old `dt=` partitions |
| Backup | Optional `hdfs dfs -get` of `/travelradar/cleaned` to host |
| Reset cluster | `docker compose down -v` wipes HDFS — document clearly |
| Tiny files | Enforce repartition before Parquet write; compact weekly if needed |

---

## 6. Hive (phase 2 — optional but report-friendly)

Once Parquet lands on HDFS:

1. Add Hive Metastore + HiveServer2 **or** use Spark SQL only with `CREATE TABLE … USING PARQUET LOCATION 'hdfs://…'`.
2. External tables:

```sql
CREATE TABLE cleaned_flights (
  icao24 STRING, callsign STRING, latitude DOUBLE, longitude DOUBLE, …
)
USING PARQUET
PARTITIONED BY (dt STRING, hour STRING)
LOCATION 'hdfs://namenode:8020/travelradar/cleaned/flights';
```

3. For MS2 report: even **Spark SQL over HDFS paths** without full Hive is acceptable if diagram shows “HDFS (+ Hive catalog planned/optional)” — prefer at least one external table demo for the presentation.

---

## 7. Phased implementation

### Phase 0 — Prep (½ day)

- [ ] Confirm Docker memory (≥8 GB; ideally 10–12 GB if possible)
- [ ] Choose Hadoop image tags; pin versions in Compose
- [ ] Add `--profile warehouse` skeleton (namenode/datanode + volumes)
- [ ] Document start/stop in root README

### Phase 1 — Cluster up (½–1 day)

- [ ] NameNode format + DataNode healthy
- [ ] Web UI on `:9870`
- [ ] `init-dirs.sh` creates `/travelradar/...`
- [ ] Spark can `hdfs dfs -ls` / write a tiny test Parquet file

### Phase 2 — Dual-sink existing jobs (1–2 days)

- [ ] Mount/add `core-site.xml` for Spark
- [ ] Update `opensky_job.py` → Parquet append under `cleaned/flights`
- [ ] Update `aisstream_job.py` → `cleaned/ships`
- [ ] Relocate checkpoints to HDFS
- [ ] Verify Redis path still works unchanged
- [ ] Capture screenshots/logs for `TeamName_Tests.pdf`

### Phase 3 — Batch / history path (1–2 days, aligns with MS2 feeds)

- [ ] Loader job: local/HTTP Finnish rail archive → `/travelradar/batch/digitraffic_rail_archive/dt=…`
- [ ] (When ready) streaming cleaned rail/bus → `cleaned/vehicles`
- [ ] Simple Spark SQL query notebook/script for EDA (counts by day, null rates)

### Phase 4 — Hardening & report (½ day)

- [ ] Resource limits + profile documented
- [ ] Architecture diagram: **Lambda** — blue stream (→ Redis), orange batch/history (→ HDFS)
- [ ] Dataset section: path, format, partition, retention
- [ ] Functional tests FT-HDFS-01… (see below)

---

## 8. Test cases (for Tests PDF)

| ID | Objective | Steps | Expected |
|----|-----------|-------|----------|
| FT-HDFS-01 | Cluster health | `compose --profile warehouse up`; open `:9870` | NameNode live; 1 Live Datanode |
| FT-HDFS-02 | Dir bootstrap | Run init script; `hdfs dfs -ls /travelradar` | `raw`, `cleaned`, `batch`, `checkpoints` exist |
| FT-HDFS-03 | Spark write | Submit job or `spark-submit` write test Parquet | Files visible under `cleaned/.../dt=…` |
| FT-HDFS-04 | Dual sink | Run `opensky_job` with traffic | Redis snaps update **and** HDFS partition grows |
| FT-HDFS-05 | Read-back | `spark.read.parquet(...)` count | Count > 0; schema matches job output |
| QT-HDFS-01 | Partition hygiene | Inspect paths after 30+ min | Partitions by `dt`/`hour`; no empty root dumps |
| QT-HDFS-02 | Schema quality | Sample Parquet | Required fields non-null at agreed rates; types correct |
| QT-HDFS-03 | Persistence | Restart Spark (keep HDFS volumes) | Historical Parquet still readable |

---

## 9. Report / presentation talking points

1. **Why HDFS:** durable history for EDA, offline ML, and Lambda batch layer; Redis alone is insufficient (24 h TTL).
2. **What we store:** cleaned Parquet events (+ later archives/timetables/models).
3. **How data gets there:** same Spark micro-batches that feed Redis also append to HDFS.
4. **Lambda alignment:** stream serving vs batch/history explicitly marked on the diagram.
5. **Limits:** single-node RF=1, academic scale; not a production HA cluster.

---

## 10. Risks and mitigations

| Risk | Mitigation |
|------|------------|
| Host OOM with NiFi+Kafka+Spark+HDFS | Compose profile; lower heaps; stop NiFi when only querying HDFS |
| Tiny Parquet files | Time-based partition + coalesce; periodic compact job |
| Hadoop/Spark version mismatch | Pin tags; smoke-test write/read in Phase 1 before job changes |
| Checkpoint loss on Spark recreate | Store checkpoints on HDFS volume |
| Scope creep into full Hive/YARN | Defer HiveServer; use Spark SQL + external tables first |

---

## 11. Definition of done

HDFS is “implemented” for this milestone when:

1. NameNode + DataNode run via Compose (documented profile).
2. `/travelradar` directory tree exists.
3. At least **flights and ships** cleaned streams land as partitioned Parquet on HDFS **without breaking** Redis/API.
4. One read-back query/script proves data is queryable.
5. Tests FT-HDFS-01–05 (and quality samples) have evidence for the Tests PDF.
6. Architecture diagram in the Report shows HDFS on the **batch/history** leg of Lambda.

---

## 12. Suggested file additions (when implementing)

```
docker-compose.yaml          # namenode, datanode, volumes, profile
hdfs/
  init-dirs.sh
  core-site.xml              # shared or copied into spark/conf
  hdfs-site.xml              # dfs.replication=1, etc.
spark/
  conf/core-site.xml         # or Dockerfile COPY
  jobs/opensky_job.py        # dual sink
  jobs/aisstream_job.py      # dual sink
  jobs/hdfs_smoke_write.py   # optional one-shot test
documentation/
  HDFS_Implementation_Plan.md  # this file
README.md                    # warehouse profile + UI port 9870
```

---

## Implementation status

| Phase | Status | Notes |
|-------|--------|-------|
| Phase 0–1 (Compose + init) | **Done in repo** | `warehouse` profile, `hdfs/`, init script |
| Phase 2 (dual-sink jobs) | **Done in repo** | `opensky_job` / `aisstream_job` → Redis + Parquet; checkpoints on HDFS |
| Smoke job | **Done** | `spark/jobs/hdfs_smoke_write.py` |
| Verify on Docker | **Pending host** | Start Docker Desktop, then run commands below |

```bash
docker compose --profile warehouse up -d
cd spark && ./submit.sh hdfs_smoke_write.py
docker compose exec namenode hdfs dfs -ls -R /travelradar
```
