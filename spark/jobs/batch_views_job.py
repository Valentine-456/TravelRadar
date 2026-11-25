"""Lambda batch layer: recompute batch views from the immutable master dataset on HDFS.

    ./submit.sh batch_views_job.py                       # yesterday and today (UTC)
    ./submit.sh batch_views_job.py --dt 2026-11-25       # one day
    ./submit.sh batch_views_job.py --all                 # every day on HDFS

Views (HDFS views/<name>, Hive tables of the same name), each overwritten per day:

    source_volume_daily    records, bytes, distinct keys and inflow rate per topic and day
                           (data set description: volume and velocity)
    quality_daily          result of every quality rule per cleaned table and day
    consistency_daily      cleaned rows from the stream vs. rows recomputed from raw by batch:
                           shows that the batch layer can rebuild the speed layer's output
    rail_delay_daily       delay statistics per train category and day from measured stop times,
                           live stream and history backfill together (EDA and ML baselines)
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from travelradar import config, quality
from travelradar.session import get_spark
from travelradar.sinks import raw_path
from travelradar.sources import ALL


QUALITY_SCHEMA = ("table string, column string, check string, failed bigint, total bigint, "
                  "rate double, dt string")
CONSISTENCY_SCHEMA = "table string, stream_rows bigint, batch_rows bigint, dt string"


def read_parquet(spark: SparkSession, path: str) -> DataFrame | None:
    try:
        return spark.read.parquet(path)
    except AnalysisException:   # nothing written yet
        return None


def only_days(df: DataFrame, days: list[str] | None) -> DataFrame:
    return df if days is None else df.where(F.col("dt").isin(days))


def with_neighbours(days: list[str] | None) -> list[str] | None:
    if days is None:
        return None
    out = set()
    for d in days:
        day = datetime.strptime(d, "%Y-%m-%d").date()
        out.update((day + timedelta(days=k)).isoformat() for k in (-1, 0, 1))
    return sorted(out)


def write_view(df: DataFrame, name: str) -> None:
    df.coalesce(1).write.mode("overwrite").partitionBy("dt").parquet(f"{config.VIEWS_ROOT}/{name}")
    print(f"{name}: {df.count()} rows", flush=True)


def source_volume(spark: SparkSession, days) -> DataFrame | None:
    frames = []
    for source in ALL.values():
        raw = read_parquet(spark, raw_path(source.topic))
        if raw is None:
            continue
        frames.append(only_days(raw, days).groupBy("dt").agg(
            F.lit(source.topic).alias("topic"),
            F.count(F.lit(1)).alias("records"),
            F.sum(F.length("value")).alias("bytes"),
            F.countDistinct("key").alias("distinct_keys"),
            F.min("ingest_time").alias("first_ingest"),
            F.max("ingest_time").alias("last_ingest"),
        ))
    if not frames:
        return None
    df = frames[0]
    for f in frames[1:]:
        df = df.unionByName(f)
    active_min = (F.col("last_ingest").cast("long") - F.col("first_ingest").cast("long")) / 60
    return df.withColumn("records_per_min", F.round(F.col("records") / F.greatest(active_min, F.lit(1)), 2))


def quality_view(spark: SparkSession, days) -> DataFrame | None:
    rows = []
    for source in ALL.values():
        cleaned = read_parquet(spark, config.cleaned_path(source.table))
        if cleaned is None:
            continue
        cleaned = only_days(cleaned, days)
        for (dt,) in cleaned.select("dt").distinct().collect():
            day = cleaned.where(F.col("dt") == dt)
            for r in quality.evaluate(day, source.table, source.quality_rules, source.dedup_keys):
                rows.append((r.table, r.column, r.check, r.failed, r.total, round(r.rate, 6), dt))
    return spark.createDataFrame(rows, QUALITY_SCHEMA) if rows else None


def consistency(spark: SparkSession, days) -> DataFrame | None:
    rows = []
    for source in ALL.values():
        raw = read_parquet(spark, raw_path(source.topic))
        cleaned = read_parquet(spark, config.cleaned_path(source.table))
        if raw is None or cleaned is None:
            continue
        # Recompute the silver table from bronze with the same parse function, batch-style.
        # Raw is partitioned by ingest day, cleaned by event day: read the neighbouring raw days
        # too, then compare on the event day.
        rebuilt = source.parse(only_days(raw, with_neighbours(days)).select("value", "ingest_time"))
        if source.dedup_keys:
            rebuilt = rebuilt.dropDuplicates(list(source.dedup_keys))
        rebuilt = only_days(rebuilt, days)
        a = only_days(cleaned, days).groupBy("dt").count().withColumnRenamed("count", "stream_rows")
        b = rebuilt.groupBy("dt").count().withColumnRenamed("count", "batch_rows")
        for row in a.join(b, "dt", "full_outer").collect():
            rows.append((source.table, row["stream_rows"] or 0, row["batch_rows"] or 0, row["dt"]))
    if not rows:
        return None
    df = spark.createDataFrame(rows, CONSISTENCY_SCHEMA)
    return df.withColumn("diff_pct", F.round(
        (F.col("stream_rows") - F.col("batch_rows")) * 100 / F.greatest(F.col("batch_rows"), F.lit(1)), 3))


def rail_delay(spark: SparkSession, days) -> DataFrame | None:
    parts = [read_parquet(spark, config.cleaned_path("train_stop_events")),
             read_parquet(spark, f"{config.BATCH_ROOT}/train_stop_events_history")]
    parts = [only_days(p, days) for p in parts if p is not None]
    if not parts:
        return None
    cols = ["train_id", "stop_seq", "train_category", "delay_min", "is_actual", "cancelled_stop",
            "stop_type", "ingest_time", "dt"]
    events = parts[0].select(*cols)
    for p in parts[1:]:
        events = events.unionByName(p.select(*cols))
    # final state per stop: the newest measured row
    final = (events.where(F.col("is_actual") & ~F.col("cancelled_stop")
                          & (F.col("stop_type") == "ARRIVAL"))
             .groupBy("dt", "train_id", "stop_seq", "train_category")
             .agg(F.max_by("delay_min", "ingest_time").alias("delay_min")))
    return final.groupBy("dt", "train_category").agg(
        F.count(F.lit(1)).alias("arrivals"),
        F.round(F.avg("delay_min"), 3).alias("mean_delay_min"),
        F.round(F.stddev("delay_min"), 3).alias("std_delay_min"),
        F.percentile_approx("delay_min", 0.5).alias("p50_delay_min"),
        F.percentile_approx("delay_min", 0.9).alias("p90_delay_min"),
        F.round(F.avg((F.col("delay_min") >= 3).cast("double")), 4).alias("late_share"),
        F.min("delay_min").alias("min_delay_min"),
        F.max("delay_min").alias("max_delay_min"),
    )


VIEWS = {
    "source_volume_daily": source_volume,
    "quality_daily": quality_view,
    "consistency_daily": consistency,
    "rail_delay_daily": rail_delay,
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt", action="append", help="day (YYYY-MM-DD); repeatable")
    ap.add_argument("--all", action="store_true", help="every day on HDFS")
    ap.add_argument("--view", action="append", choices=list(VIEWS), help="default: all views")
    args = ap.parse_args()
    if args.all:
        days = None
    elif args.dt:
        days = args.dt
    else:
        today = datetime.now(timezone.utc).date()
        days = [(today - timedelta(days=1)).isoformat(), today.isoformat()]

    spark = get_spark("BatchViews")
    for name in args.view or VIEWS:
        df = VIEWS[name](spark, days)
        if df is None:
            print(f"{name}: no input yet, skipped", flush=True)
        else:
            write_view(df, name)
    spark.stop()


if __name__ == "__main__":
    main()
