"""Digitraffic rail timetables with live and actual times (Finland).

A record is one complete train (https://rata.digitraffic.fi/api/v1/trains/<date>/<number>):
the MQTT topic `trains/#` pushes the whole train again every time any of its rows changes, and the
daily history archive returns the same objects. `parse` explodes it into one *stop event* per
commercial stop (arrival or departure at a passenger station).

Delay labels: rows with `actual_time` set are measured (ground truth for the delay model);
rows with only `estimate_time` are Digitraffic's own forecast (the operator baseline).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, BooleanType, IntegerType, LongType, StringType,
                               StructField, StructType)

from travelradar.common import Source, blank_to_null, iso_ts

ROW = StructType([
    StructField("stationShortCode", StringType()),
    StructField("type", StringType()),
    StructField("commercialStop", BooleanType()),
    StructField("cancelled", BooleanType()),
    StructField("scheduledTime", StringType()),
    StructField("liveEstimateTime", StringType()),
    StructField("actualTime", StringType()),
    StructField("differenceInMinutes", IntegerType()),
    StructField("estimateSource", StringType()),
    StructField("commercialTrack", StringType()),
    StructField("causes", ArrayType(StructType([StructField("categoryCode", StringType())]))),
])

TRAIN = StructType([
    StructField("trainNumber", IntegerType()),
    StructField("departureDate", StringType()),
    StructField("operatorShortCode", StringType()),
    StructField("trainCategory", StringType()),
    StructField("trainType", StringType()),
    StructField("commuterLineID", StringType()),
    StructField("runningCurrently", BooleanType()),
    StructField("cancelled", BooleanType()),
    StructField("version", LongType()),
    StructField("timeTableRows", ArrayType(ROW)),
])


def parse(frame: DataFrame) -> DataFrame:
    t = F.col("t")
    trains = frame.select(F.from_json("value", TRAIN).alias("t"), "ingest_time") \
        .where(t["trainNumber"].isNotNull() & t["departureDate"].isNotNull())

    # posexplode keeps the row order: stop_seq orders the stops along the route
    rows = trains.select("t", "ingest_time",
                         F.posexplode(t["timeTableRows"]).alias("stop_seq", "r"))
    r = F.col("r")
    scheduled = iso_ts(r["scheduledTime"])
    actual = iso_ts(r["actualTime"])
    estimate = iso_ts(r["liveEstimateTime"])
    # differenceInMinutes is given whenever an actual or estimated time exists; derive it otherwise
    derived = F.round((F.coalesce(actual, estimate).cast("long") - scheduled.cast("long")) / 60) \
        .cast("int")
    out = rows.where(r["commercialStop"]).select(
        F.concat_ws("_", t["departureDate"], t["trainNumber"].cast("string")).alias("train_id"),
        t["trainNumber"].alias("train_number"),
        t["departureDate"].alias("departure_date"),
        t["operatorShortCode"].alias("operator"),
        t["trainCategory"].alias("train_category"),
        t["trainType"].alias("train_type"),
        blank_to_null(t["commuterLineID"]).alias("commuter_line"),
        F.coalesce(t["cancelled"], F.lit(False)).alias("cancelled_train"),
        t["version"].alias("train_version"),
        F.col("stop_seq"),
        r["stationShortCode"].alias("station"),
        r["type"].alias("stop_type"),
        blank_to_null(r["commercialTrack"]).alias("track"),
        scheduled.alias("scheduled_time"),
        estimate.alias("estimate_time"),
        actual.alias("actual_time"),
        F.coalesce(r["differenceInMinutes"], derived).alias("delay_min"),
        actual.isNotNull().alias("is_actual"),
        F.coalesce(r["cancelled"], F.lit(False)).alias("cancelled_stop"),
        r["estimateSource"].alias("estimate_source"),
        F.transform(r["causes"], lambda c: c["categoryCode"]).alias("cause_codes"),
        F.col("ingest_time"),
    )
    return out.where(F.col("station").isNotNull() & F.col("scheduled_time").isNotNull()) \
        .withColumn("dt", F.col("departure_date"))   # partition by operating day of the train


SOURCE = Source(
    name="digitraffic_trains",
    topic="digitraffic-trains",
    table="train_stop_events",
    parse=parse,
    # a train message repeats all rows; keep a row again only when its observable state changed
    dedup_keys=("train_id", "stop_seq", "estimate_time", "actual_time", "delay_min",
                "cancelled_stop"),
    dedup_watermark="3 hours",
    description="Scheduled, estimated and actual times of every commercial stop of every train "
                "in Finland (Digitraffic MQTT)",
    quality_rules={
        "train_id": {"not_null": True},
        "station": {"not_null": True, "regex": "^[A-ZÄÖÅ0-9]{1,6}$"},
        "stop_type": {"not_null": True, "allowed": ["ARRIVAL", "DEPARTURE"]},
        "scheduled_time": {"not_null": True},
        "delay_min": {"range": (-180, 720)},
    },
)
