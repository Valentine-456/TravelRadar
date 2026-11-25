"""Speed layer -> Redis: time-bucketed snapshots of planes and ships for the map and its time slider.

Layout per kind ("planes", "ships"), read by backend/src/backend/api/snapshots.py:
    <kind>:snaps         sorted set of snapshot times (Unix seconds)
    <kind>:snap:<time>   hash, object id -> JSON, expires 24 h after the snapshot time
"""
from __future__ import annotations

import time
from typing import Iterable

import redis
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from travelradar import config
from travelradar.common import kafka_frame
from travelradar.sinks import read_kafka, with_trigger
from travelradar.sources import ais, opensky

FLIGHT_BUCKET_SECONDS = 60
SHIP_BUCKET_SECONDS = 5 * 60    # ships move slowly

FLIGHT_FIELDS = ["icao24", "callsign", "origin_country", "longitude", "latitude", "altitude",
                 "velocity", "true_track"]
SHIP_FIELDS = ["mmsi", "ship_name", "latitude", "longitude", "speed_knots", "heading", "nav_status"]


def _bucket(ts_col: str, seconds: int):
    return (F.floor(F.col(ts_col).cast("long") / seconds) * seconds).cast("long")


def flight_snapshot_rows(cleaned: DataFrame) -> DataFrame:
    """One row per (minute, aircraft): the newest fix received in that minute."""
    return (cleaned.where(F.col("true_track").isNotNull())   # the map rotates icons by track
            .withColumn("bucket", _bucket("ingest_time", FLIGHT_BUCKET_SECONDS))
            .groupBy("bucket", "icao24")
            .agg(F.max_by(F.to_json(F.struct(*FLIGHT_FIELDS)), "event_time").alias("data"))
            .select("bucket", F.col("icao24").alias("id"), "data"))


def ship_snapshot_rows(cleaned: DataFrame) -> DataFrame:
    """One row per (5-minute bucket, ship): the newest report received in that bucket."""
    return (cleaned
            .withColumn("bucket", _bucket("ingest_time", SHIP_BUCKET_SECONDS))
            .groupBy("bucket", "mmsi")
            .agg(F.max_by(F.to_json(F.struct(*SHIP_FIELDS)), "event_time").alias("data"))
            .select("bucket", F.col("mmsi").cast("string").alias("id"), "data"))


def write_snapshots(client: redis.Redis, kind: str, rows: Iterable,
                    retention: int = config.HOT_RETENTION_SECONDS) -> int:
    pipe = client.pipeline(transaction=False)
    n = 0
    for row in rows:
        key = f"{kind}:snap:{row.bucket}"
        pipe.hset(key, row.id, row.data)
        pipe.expireat(key, row.bucket + retention)
        pipe.zadd(f"{kind}:snaps", {str(row.bucket): row.bucket})
        n += 1
    pipe.execute()
    return n


def trim_snapshots(client: redis.Redis, kind: str, now: float | None = None,
                   retention: int = config.HOT_RETENTION_SECONDS) -> int:
    """Drop index entries whose hash has expired."""
    now = time.time() if now is None else now
    return client.zremrangebyscore(f"{kind}:snaps", "-inf", now - retention)


def _redis() -> redis.Redis:
    return redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT)


def batch_writer(kind: str, to_rows, client_factory=None):
    """foreachBatch function: snapshot rows -> Redis, then drop expired index entries.

    A batch holds at most one row per object and bucket (a few hundred planes, a few thousand
    ships), so the rows are written from the driver in one pipelined round trip."""
    key_kind = f"{config.REDIS_PREFIX}{kind}"

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        client = (client_factory or _redis)()
        n = write_snapshots(client, key_kind, to_rows(batch_df).toLocalIterator())
        trim_snapshots(client, key_kind)
        print(f"hot_{kind} batch {batch_id}: {n} snapshot rows", flush=True)
    return write_batch


HOT = {"planes": (opensky.SOURCE, flight_snapshot_rows),
       "ships": (ais.SOURCE, ship_snapshot_rows)}


def start_hot(spark: SparkSession, kind: str, trigger: str | None = None,
              kafka_df: DataFrame | None = None) -> StreamingQuery:
    source, to_rows = HOT[kind]
    kafka_df = kafka_df if kafka_df is not None else read_kafka(spark, [source.topic])
    cleaned = source.parse(kafka_frame(kafka_df))
    writer = (cleaned.writeStream
              .queryName(f"hot_{kind}")
              .foreachBatch(batch_writer(kind, to_rows))
              .option("checkpointLocation", config.checkpoint_path(f"hot_{kind}")))
    return with_trigger(writer, trigger or config.HOT_TRIGGER).start()
