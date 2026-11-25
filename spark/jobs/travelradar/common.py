"""Shared pieces of the source modules.

Every source module exposes a `SOURCE` (see `Source`) whose `parse` turns a *Kafka frame* into the
cleaned table. A Kafka frame has at least two columns:

    value        string     the record exactly as NiFi published it
    ingest_time  timestamp  when it reached Kafka

The streaming pipeline, the batch backfills and the unit tests all build that frame and call the
same `parse`, so one piece of code defines the transformation everywhere.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Source:
    name: str                                   # short name, used for checkpoints
    topic: str                                  # Kafka topic
    table: str                                  # cleaned table (HDFS folder and Hive table)
    parse: Callable[[DataFrame], DataFrame]     # Kafka frame -> cleaned rows, incl. `dt`
    dedup_keys: tuple[str, ...] = ()            # identical events within the watermark are dropped
    dedup_watermark: str = "30 minutes"
    description: str = ""
    quality_rules: dict = field(default_factory=dict)  # column -> rule, see quality.py


def kafka_frame(kafka_df: DataFrame) -> DataFrame:
    """Raw Kafka source rows -> Kafka frame (value as text, plus the Kafka metadata)."""
    return kafka_df.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        F.col("timestamp").alias("ingest_time"),
    )


def utc_date(ts: Column) -> Column:
    """Partition value `dt` (YYYY-MM-DD, UTC; the session time zone is pinned to UTC)."""
    return F.date_format(ts, "yyyy-MM-dd")


def blank_to_null(c: Column) -> Column:
    trimmed = F.trim(c)
    return F.when(trimmed == "", None).otherwise(trimmed)


def iso_ts(c: Column) -> Column:
    """ISO-8601 text such as 2026-11-25T10:01:02.000Z or 2026-11-25T10:01:02+01:00 -> timestamp."""
    return F.to_timestamp(c)
