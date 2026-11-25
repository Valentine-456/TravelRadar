"""OpenSky Network state vectors (flights).

NiFi splits each `/states/all` response into one Kafka record per aircraft. A record is the raw
state-vector array (https://openskynetwork.github.io/opensky-api/rest.html#all-state-vectors):

    0 icao24  1 callsign  2 origin_country  3 time_position  4 last_contact  5 longitude
    6 latitude  7 baro_altitude  8 on_ground  9 velocity  10 true_track  11 vertical_rate
    12 sensors  13 geo_altitude  14 squawk  15 spi  16 position_source
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from travelradar.common import Source, blank_to_null, utc_date


def _num(j, i):
    return j[i].cast("double")


def parse(frame: DataFrame) -> DataFrame:
    j = F.col("j")
    parsed = frame.select(
        F.from_json("value", ArrayType(StringType())).alias("j"), "ingest_time"
    ).where(F.size(j) >= 17)  # malformed or truncated vectors are dropped

    time_position = F.timestamp_seconds(j[3].cast("long"))
    last_contact = F.timestamp_seconds(j[4].cast("long"))
    out = parsed.select(
        F.lower(F.trim(j[0])).alias("icao24"),
        blank_to_null(j[1]).alias("callsign"),
        blank_to_null(j[2]).alias("origin_country"),
        time_position.alias("time_position"),
        last_contact.alias("last_contact"),
        # time of the position fix; falls back to the last message when no fix time is given
        F.coalesce(time_position, last_contact).alias("event_time"),
        _num(j, 5).alias("longitude"),
        _num(j, 6).alias("latitude"),
        _num(j, 7).alias("baro_altitude"),
        _num(j, 13).alias("geo_altitude"),
        F.coalesce(_num(j, 13), _num(j, 7)).alias("altitude"),
        (F.lower(j[8]) == "true").alias("on_ground"),
        _num(j, 9).alias("velocity"),
        _num(j, 10).alias("true_track"),
        _num(j, 11).alias("vertical_rate"),
        blank_to_null(j[14]).alias("squawk"),
        j[16].cast("int").alias("position_source"),
        F.col("ingest_time"),
    )
    return out.where(
        F.col("icao24").rlike("^[0-9a-f]{6}$")
        & F.col("event_time").isNotNull()
        & F.col("latitude").between(-90, 90)
        & F.col("longitude").between(-180, 180)
    ).withColumn("dt", utc_date("event_time"))


SOURCE = Source(
    name="opensky",
    topic="opensky",
    table="flight_positions",
    parse=parse,
    dedup_keys=("icao24", "event_time"),   # an aircraft without a new fix repeats its last vector
    description="Aircraft positions over the Nordics (OpenSky, polled every 120 s)",
    quality_rules={
        "icao24": {"not_null": True, "regex": "^[0-9a-f]{6}$"},
        "latitude": {"not_null": True, "range": (47.0, 71.5)},     # NiFi request bounding box
        "longitude": {"not_null": True, "range": (4.5, 32.0)},
        "altitude": {"range": (-500.0, 20000.0)},
        "velocity": {"range": (0.0, 400.0)},                       # m/s
        "true_track": {"range": (0.0, 360.0)},
        "event_time": {"not_null": True, "max_lag_seconds": 600},  # fix older than 10 min = stale
    },
)
