"""AISStream position reports (ships).

NiFi forwards every WebSocket message unchanged. AIS encodes "not available" with sentinel values,
which are turned into nulls here (ITU-R M.1371): latitude 91, longitude 181, speed over ground 102.3,
course over ground 360, true heading 511, rate of turn -128.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

from travelradar.common import Source, blank_to_null, utc_date

SCHEMA = StructType([
    StructField("MessageType", StringType()),
    StructField("MetaData", StructType([
        StructField("MMSI", LongType()),
        StructField("ShipName", StringType()),
        StructField("time_utc", StringType()),
    ])),
    StructField("Message", StructType([
        StructField("PositionReport", StructType([
            StructField("Latitude", DoubleType()),
            StructField("Longitude", DoubleType()),
            StructField("Sog", DoubleType()),
            StructField("Cog", DoubleType()),
            StructField("TrueHeading", IntegerType()),
            StructField("NavigationalStatus", IntegerType()),
            StructField("RateOfTurn", IntegerType()),
            StructField("PositionAccuracy", BooleanType()),
        ])),
    ])),
])


def parse(frame: DataFrame) -> DataFrame:
    m = F.col("m")
    r = m["Message"]["PositionReport"]
    parsed = frame.select(F.from_json("value", SCHEMA).alias("m"), "ingest_time") \
        .where(m["MessageType"] == "PositionReport")

    true_heading = F.when(r["TrueHeading"].between(0, 359), r["TrueHeading"].cast("double"))
    course = F.when((r["Cog"] >= 0) & (r["Cog"] < 360), r["Cog"])
    out = parsed.select(
        m["MetaData"]["MMSI"].alias("mmsi"),
        blank_to_null(m["MetaData"]["ShipName"]).alias("ship_name"),
        # "2026-11-22 01:03:43.792353774 +0000 UTC": always UTC; keep up to microseconds
        F.regexp_extract(m["MetaData"]["time_utc"],
                         r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{1,6})?)", 1)
         .cast("timestamp").alias("event_time"),
        r["Latitude"].alias("latitude"),
        r["Longitude"].alias("longitude"),
        F.when((r["Sog"] >= 0) & (r["Sog"] < 102.3), r["Sog"]).alias("speed_knots"),
        course.alias("course"),
        true_heading.alias("true_heading"),
        F.coalesce(true_heading, course).alias("heading"),   # what the map uses to rotate the icon
        r["NavigationalStatus"].alias("nav_status"),
        F.when(r["RateOfTurn"] != -128, r["RateOfTurn"]).alias("rate_of_turn"),
        r["PositionAccuracy"].alias("position_accuracy"),
        F.col("ingest_time"),
    )
    return out.where(
        F.col("mmsi").between(100_000_000, 999_999_999)
        & F.col("event_time").isNotNull()
        & F.col("latitude").between(-90, 90)        # also drops the 91 sentinel
        & F.col("longitude").between(-180, 180)     # also drops the 181 sentinel
    ).withColumn("dt", utc_date("event_time"))


SOURCE = Source(
    name="ais",
    topic="ais",
    table="ship_positions",
    parse=parse,
    dedup_keys=("mmsi", "event_time"),   # the same report can arrive through several receivers
    description="Ship position reports in the Nordic and Baltic seas (AISStream WebSocket)",
    quality_rules={
        "mmsi": {"not_null": True, "range": (100_000_000, 999_999_999)},
        "latitude": {"not_null": True, "range": (47.0, 71.5)},   # subscription bounding box
        "longitude": {"not_null": True, "range": (4.5, 32.0)},
        "speed_knots": {"range": (0.0, 60.0)},
        "heading": {"range": (0.0, 360.0)},
        "nav_status": {"range": (0, 15)},
        "event_time": {"not_null": True, "max_lag_seconds": 600},
    },
)
