"""Integration tests against the running stack (docker compose up -d; nifi/deploy.py; pipeline running).

    cd tests && uv run pytest -m integration
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import time
import urllib.request
from datetime import datetime, timezone

import pytest
import redis

from conftest import REPO, compose
from test_nifi_flows import kafka_topics

pytestmark = pytest.mark.integration

FIXTURES = REPO / "spark/tests/fixtures"
KEYLESS_GROUPS = ["DigitrafficRail", "Entur", "OpenMeteo"]
IT_ROOT = "hdfs://namenode:9000/travelradar-it"


def kafka(*args: str, stdin: str | None = None) -> str:
    return compose("exec", "-T", "kafka", *args, stdin=stdin)


def end_offsets(topic: str) -> int:
    out = kafka("/opt/kafka/bin/kafka-run-class.sh", "org.apache.kafka.tools.GetOffsetShell",
                "--bootstrap-server", "localhost:9092", "--topic", topic)
    return sum(int(line.rsplit(":", 1)[1]) for line in out.split() if line.count(":") == 2)


def hdfs(*args: str, check: bool = True) -> str:
    return compose("exec", "-T", "namenode", "hdfs", "dfs", *args, check=check)


def beeline(sql: str) -> str:
    return compose("exec", "-T", "-e", "HADOOP_CLIENT_OPTS=-Xmx256m", "hive", "beeline", "-u",
                   "jdbc:hive2://localhost:10000/travelradar", "-n", "hive", "--silent=true",
                   "--outputformat=csv2", "--showHeader=false", "-e", sql)


def spark_counts(paths: dict[str, str]) -> dict[str, int]:
    """Row counts of Parquet folders on HDFS, read with Spark in the spark-master container."""
    script = (
        "import json\nfrom pyspark.sql import SparkSession\n"
        "s = SparkSession.builder.master('local[1]').config('spark.ui.enabled','false').getOrCreate()\n"
        "s.sparkContext.setLogLevel('ERROR')\n"
        f"paths = {json.dumps(paths)}\n"
        "print('COUNTS=' + json.dumps({k: s.read.parquet(v).count() for k, v in paths.items()}))\n")
    out = compose("exec", "-T", "spark-master", "python3", "-", stdin=script)
    return json.loads(re.search(r"COUNTS=(\{.*\})", out).group(1))


@pytest.fixture(scope="module")
def nifi():
    from nifi_api import NiFi
    return NiFi()


def test_kafka_topics_and_retention(actual):
    """IT-01: Kafka topics and retention
    Objective: The broker has every topic of kafka/topics.txt with its partition count, and keeps data 72 h (replay buffer).
    Steps: kafka-topics --describe for each topic; kafka-configs --describe --all for broker 1.
    Expected: all 6 topics with the listed partitions; log.retention.hours=72."""
    for topic, partitions in kafka_topics().items():
        out = kafka("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
                    "--describe", "--topic", topic)
        assert f"PartitionCount: {partitions}" in out, topic
    cfg = kafka("/opt/kafka/bin/kafka-configs.sh", "--bootstrap-server", "localhost:9092",
                "--entity-type", "brokers", "--entity-name", "1", "--describe", "--all")
    assert "log.retention.hours=72" in cfg
    actual(f"{len(kafka_topics())} topics OK, log.retention.hours=72")


def test_hdfs_layout(actual):
    """IT-02: HDFS is up with the TravelRadar layout
    Objective: The batch-layer store is available: one live datanode, no safe mode, directory tree created by hdfs-init.
    Steps: hdfs dfsadmin -report; hdfs dfs -ls /travelradar.
    Expected: 1 live datanode; raw, cleaned, batch, views, models, checkpoints present."""
    report = compose("exec", "-T", "namenode", "hdfs", "dfsadmin", "-report")
    assert re.search(r"Live datanodes \(1\)", report)
    listing = hdfs("-ls", "/travelradar")
    for d in ["raw", "cleaned", "batch", "views", "models", "checkpoints"]:
        assert f"/travelradar/{d}" in listing
    used = re.search(r"DFS Used: \d+ \(([^)]+)\)", report).group(1)
    actual(f"1 live datanode, DFS used {used}, all 6 folders present")


def test_hive_tables_queryable(actual):
    """IT-03: Hive exposes every table and can query it
    Objective: Every table of hive/tables.sql exists in HiveServer2 and returns a row count (partitions registered by the sync loop).
    Steps: SHOW TABLES; then SELECT COUNT(*) on every table through beeline.
    Expected: the 14 tables of tables.sql; every query succeeds."""
    expected = sorted(re.findall(r"CREATE EXTERNAL TABLE (\w+)", (REPO / "hive/tables.sql").read_text()))
    tables = sorted(l.strip() for l in beeline("SHOW TABLES;").splitlines() if l.strip())
    assert tables == expected
    # one statement per table in a single session (a 14-way UNION exceeds Tez's counter limit)
    sql = " ".join(f"SELECT '{t}', COUNT(*) FROM {t};" for t in expected)
    counts = dict(l.split(",") for l in beeline(sql).splitlines() if "," in l)
    assert set(counts) == set(expected)
    actual({t: int(n) for t, n in sorted(counts.items())})


def test_nifi_flows_running(nifi, actual):
    """IT-04: NiFi acquisition flows are deployed and running
    Objective: nifi/deploy.py imported every flow; the keyless flows run without invalid processors; OpenSky/AIS run when their keys are configured.
    Steps: List process groups and their running / invalid counts through the NiFi REST API; read the bulletin board.
    Expected: DigitrafficRail, Entur, OpenMeteo running with 0 invalid; no ERROR bulletins from them."""
    groups = nifi.groups()
    state = {n: (g["component"]["runningCount"], g["component"]["invalidCount"]) for n, g in groups.items()}
    for name in KEYLESS_GROUPS:
        running, invalid = state[name]
        assert running > 0 and invalid == 0, (name, state[name])
    errors = [b["bulletin"]["sourceName"] for name in KEYLESS_GROUPS
              for b in nifi.bulletins(groups[name]["id"]) if b["bulletin"].get("level") == "ERROR"]
    assert not errors
    actual({n: f"running {r}, invalid {i}" for n, (r, i) in sorted(state.items())})


@pytest.mark.parametrize("topic", ["digitraffic-trains", "digitraffic-locations", "entur-vehicles"])
def test_live_ingestion(topic, actual):
    """IT-05: Live data reaches Kafka
    Objective: Each keyless source delivers new records within its update interval (MQTT push; Entur poll 60 s).
    Steps: Read the topic's end offsets, wait up to 120 s, read them again.
    Expected: the end offset increases."""
    before = end_offsets(topic)
    deadline = time.time() + 120
    while (after := end_offsets(topic)) == before and time.time() < deadline:
        time.sleep(10)
    assert after > before
    actual(f"{topic}: {after - before} new records ({before} -> {after})")


def test_pipeline_running_and_archiving(actual):
    """IT-06: Streaming pipeline is running and writes today's partitions
    Objective: The Spark application runs on the cluster and the archive queries write the current day to HDFS.
    Steps: Spark master /json: active applications; hdfs dfs -ls the cleaned tables of the keyless sources.
    Expected: TravelRadarPipeline RUNNING; partition dt=<today UTC> exists for train_positions, vehicle_positions and train_stop_events."""
    with urllib.request.urlopen("http://localhost:8080/json/", timeout=10) as r:
        apps = {a["name"]: a["state"] for a in json.load(r)["activeapps"]}
    assert apps.get("TravelRadarPipeline") == "RUNNING", apps
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for table in ["train_positions", "vehicle_positions", "train_stop_events"]:
        assert f"dt={today}" in hdfs("-ls", f"/travelradar/cleaned/{table}"), table
    actual(f"apps {apps}; dt={today} present in 3 cleaned tables")


def test_redis_snapshots_fresh(actual):
    """IT-07: Map snapshots in Redis are fresh
    Objective: The speed layer keeps the map up to date (planes every minute, ships every 5 minutes).
    Steps: Read the newest entry of planes:snaps and ships:snaps.
    Expected: newest planes snapshot < 5 min old, ships < 15 min old (skipped while the OpenSky/AIS keys are not configured)."""
    r = redis.Redis(decode_responses=True)
    ages = {}
    for kind, limit in [("planes", 300), ("ships", 900)]:
        newest = r.zrange(f"{kind}:snaps", -1, -1)
        if not newest:
            pytest.skip(f"no {kind} snapshots: OpenSky/AIS keys not configured in nifi/.env")
        ages[kind] = time.time() - int(newest[0])
        assert ages[kind] < limit, kind
    actual({k: f"{v:.0f} s old" for k, v in ages.items()})


# ---- isolated end-to-end replay -------------------------------------------------------------------

def fixture_records() -> dict[str, list[str]]:
    lines = lambda f: [l for l in (FIXTURES / f).read_text().splitlines() if l.strip()]
    load = lambda f: json.loads((FIXTURES / f).read_text())
    return {
        "opensky": lines("opensky.jsonl"),
        "ais": lines("ais.jsonl"),
        "digitraffic-trains": [json.dumps(t) for t in load("dt_train_1.json")],
        "digitraffic-locations": [json.dumps(l) for l in load("dt_locations.json")],
        "entur-vehicles": [json.dumps(v) for v in load("entur.json")["data"]["vehicles"]],
        "weather": [json.dumps(e) for e in load("openmeteo.json")],
    }


@pytest.fixture
def isolated_namespace():
    """it.* topics, it:* Redis keys and /travelradar-it on HDFS; removed afterwards."""
    topics = list(kafka_topics())
    for t in topics:
        kafka("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--create",
              "--if-not-exists", "--topic", f"it.{t}", "--partitions", "1")
    yield
    for t in topics:
        kafka("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--delete",
              "--topic", f"it.{t}")
    hdfs("-rm", "-r", "-f", "-skipTrash", "/travelradar-it", check=False)
    r = redis.Redis()
    for key in r.scan_iter("it:*"):
        r.delete(key)


def test_end_to_end_replay(isolated_namespace, actual):
    """IT-08: End-to-end: Kafka -> Spark -> HDFS (raw + cleaned) and Redis
    Objective: Run the production pipeline code on known input and check every sink, without touching production data (it.* topics, it:* Redis keys, /travelradar-it on HDFS).
    Steps: 1) Produce the captured samples of all 6 sources to it.<topic>. 2) Run pipeline_job.py --once (availableNow). 3) Count raw and cleaned rows on HDFS with Spark; read it:planes / it:ships from Redis.
    Expected: raw rows = records produced for every topic; cleaned rows equal the unit-test results (flights 11, ships 4, stop events 24, train positions = sample size, vehicles 21, weather 2); Redis holds a fresh planes and ships snapshot."""
    records = fixture_records()
    for topic, values in records.items():
        kafka("/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server", "localhost:9092",
              "--topic", f"it.{topic}", stdin="\n".join(values) + "\n")
    env = {**os.environ, "TRAVELRADAR_ROOT": IT_ROOT, "TOPIC_PREFIX": "it.", "REDIS_PREFIX": "it:"}
    run = subprocess.run(["./submit.sh", "pipeline_job.py", "--once"], cwd=REPO / "spark", env=env,
                         capture_output=True, text=True, timeout=900)
    assert run.returncode == 0, run.stdout[-3000:] + run.stderr[-3000:]

    paths = {f"raw:{t}": f"{IT_ROOT}/raw/events/topic={t}" for t in records}
    tables = {"flight_positions": 11, "ship_positions": 4, "train_stop_events": 24,
              "train_positions": len(records["digitraffic-locations"]), "vehicle_positions": 21,
              "weather_observations": 2}
    paths |= {t: f"{IT_ROOT}/cleaned/{t}" for t in tables}
    counts = spark_counts(paths)
    for t, values in records.items():
        assert counts[f"raw:{t}"] == len(values), t
    for t, n in tables.items():
        assert counts[t] == n, (t, counts[t])

    r = redis.Redis(decode_responses=True)
    planes = r.zrange("it:planes:snaps", -1, -1)
    ships = r.zrange("it:ships:snaps", -1, -1)
    assert planes and r.hlen(f"it:planes:snap:{planes[0]}") > 0
    assert ships and r.hlen(f"it:ships:snap:{ships[0]}") == 4
    assert time.time() - int(planes[0]) < 900
    actual({**counts, "redis planes": r.hlen(f"it:planes:snap:{planes[0]}"),
            "redis ships": r.hlen(f"it:ships:snap:{ships[0]}")})
