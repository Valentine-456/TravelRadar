"""Digitraffic train GPS positions (Finland), one MQTT message per position fix (topic
`train-locations/#`). Positions carry no delay; join with train_stop_events on `train_id`."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, DoubleType, IntegerType, StringType, StructField,
                               StructType)

from travelradar.common import Source, iso_ts, utc_date

SCHEMA = StructType([
    StructField("trainNumber", IntegerType()),
    StructField("departureDate", StringType()),
    StructField("timestamp", StringType()),
    StructField("speed", DoubleType()),
    StructField("accuracy", DoubleType()),
    StructField("location", StructType([
        StructField("coordinates", ArrayType(DoubleType())),
    ])),
])


def parse(frame: DataFrame) -> DataFrame:
    p = F.col("p")
    coords = p["location"]["coordinates"]
    out = frame.select(F.from_json("value", SCHEMA).alias("p"), "ingest_time").select(
        F.concat_ws("_", p["departureDate"], p["trainNumber"].cast("string")).alias("train_id"),
        p["trainNumber"].alias("train_number"),
        p["departureDate"].alias("departure_date"),
        iso_ts(p["timestamp"]).alias("event_time"),
        coords[0].alias("longitude"),
        coords[1].alias("latitude"),
        p["speed"].alias("speed_kmh"),
        p["accuracy"].alias("accuracy_m"),
        F.col("ingest_time"),
    )
    return out.where(
        F.col("train_number").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("latitude").between(-90, 90)
        & F.col("longitude").between(-180, 180)
    ).withColumn("dt", utc_date("event_time"))


SOURCE = Source(
    name="digitraffic_locations",
    topic="digitraffic-locations",
    table="train_positions",
    parse=parse,
    dedup_keys=("train_id", "event_time"),
    description="GPS position of every running train in Finland (Digitraffic MQTT)",
    quality_rules={
        "train_id": {"not_null": True},
        "latitude": {"not_null": True, "range": (59.0, 70.5)},    # Finland incl. border stations
        "longitude": {"not_null": True, "range": (19.0, 32.0)},
        "speed_kmh": {"range": (0.0, 250.0)},
        "event_time": {"not_null": True, "max_lag_seconds": 600},
    },
)
