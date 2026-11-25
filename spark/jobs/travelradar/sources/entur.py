"""Entur real-time vehicles (Norway): buses, trams, ferries, coaches, trains with SIRI delay.

NiFi polls the GraphQL API every 60 s and splits `data.vehicles` into one Kafka record per vehicle.
Known issues (PM1 data quality section): speed is mostly missing, delay of finished trips
(vehicleStatus COMPLETED) is stale, and delays of several hours occur. Rows are kept; `delay_valid`
marks the delays usable as labels.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

from travelradar.common import Source, blank_to_null, iso_ts, utc_date

MAX_PLAUSIBLE_DELAY_S = 2 * 3600

SCHEMA = StructType([
    StructField("vehicleId", StringType()),
    StructField("lastUpdated", StringType()),
    StructField("mode", StringType()),
    StructField("speed", DoubleType()),
    StructField("bearing", DoubleType()),
    StructField("delay", DoubleType()),
    StructField("monitored", BooleanType()),
    StructField("inCongestion", BooleanType()),
    StructField("occupancyStatus", StringType()),
    StructField("vehicleStatus", StringType()),
    StructField("location", StructType([
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
    ])),
    StructField("line", StructType([
        StructField("lineRef", StringType()),
        StructField("lineName", StringType()),
        StructField("publicCode", StringType()),
    ])),
    StructField("serviceJourney", StructType([
        StructField("serviceJourneyId", StringType()),
        StructField("date", StringType()),
    ])),
    StructField("operator", StructType([StructField("operatorRef", StringType())])),
    StructField("codespace", StructType([StructField("codespaceId", StringType())])),
])


def parse(frame: DataFrame) -> DataFrame:
    v = F.col("v")
    status = F.upper(v["vehicleStatus"])
    out = frame.select(F.from_json("value", SCHEMA).alias("v"), "ingest_time").select(
        v["vehicleId"].alias("vehicle_id"),
        F.upper(v["mode"]).alias("mode"),
        v["codespace"]["codespaceId"].alias("codespace"),
        F.coalesce(blank_to_null(v["operator"]["operatorRef"]),
                   v["codespace"]["codespaceId"]).alias("operator"),
        v["line"]["lineRef"].alias("line_ref"),
        v["line"]["lineName"].alias("line_name"),
        v["line"]["publicCode"].alias("line_public_code"),
        v["serviceJourney"]["serviceJourneyId"].alias("trip_id"),
        v["serviceJourney"]["date"].alias("service_date"),
        iso_ts(v["lastUpdated"]).alias("event_time"),
        v["location"]["latitude"].alias("latitude"),
        v["location"]["longitude"].alias("longitude"),
        (v["speed"] * 3.6).alias("speed_kmh"),          # Entur reports m/s
        v["bearing"].alias("bearing"),
        v["delay"].alias("delay_s"),
        (v["delay"].isNotNull()
         & (F.abs(v["delay"]) <= MAX_PLAUSIBLE_DELAY_S)
         & (F.coalesce(status, F.lit("")) != "COMPLETED")).alias("delay_valid"),
        status.alias("vehicle_status"),
        v["occupancyStatus"].alias("occupancy"),
        v["monitored"].alias("monitored"),
        v["inCongestion"].alias("in_congestion"),
        F.col("ingest_time"),
    )
    return out.where(
        F.col("vehicle_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("latitude").between(-90, 90)
        & F.col("longitude").between(-180, 180)
    ).withColumn("dt", utc_date("event_time"))


SOURCE = Source(
    name="entur",
    topic="entur-vehicles",
    table="vehicle_positions",
    parse=parse,
    # each poll returns every vehicle; a vehicle whose lastUpdated did not change is a repeat
    dedup_keys=("vehicle_id", "event_time"),
    description="Position, line and delay of public transport vehicles in Norway (Entur GraphQL, "
                "polled every 60 s)",
    quality_rules={
        "vehicle_id": {"not_null": True},
        "mode": {"allowed": ["BUS", "TRAM", "RAIL", "FERRY", "COACH", "METRO", "AIR", "WATER",
                             "TAXI", "LIFT", "UNKNOWN"]},
        "latitude": {"not_null": True, "range": (57.5, 71.5)},
        "longitude": {"not_null": True, "range": (4.0, 31.5)},
        "delay_s": {"range": (-7200.0, 7200.0)},
        "speed_kmh": {"range": (0.0, 200.0)},
        "event_time": {"not_null": True, "max_lag_seconds": 3600},
    },
)
