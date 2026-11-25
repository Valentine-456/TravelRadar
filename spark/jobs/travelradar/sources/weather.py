"""Open-Meteo current weather for 12 Nordic cities (ML features: joined to the nearest city).

NiFi requests all cities in one call every 15 minutes and splits the response array into one
Kafka record per city. The response echoes the model grid point, not our city name, so the city is
recovered as the nearest entry of `CITIES`.
"""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from travelradar.common import Source, utc_date

# id: (latitude, longitude, country). Same list as the NiFi request URL (nifi/WeatherProcessGroup.json).
CITIES = {
    "helsinki": (60.17, 24.94, "FI"), "tampere": (61.50, 23.76, "FI"), "turku": (60.45, 22.27, "FI"),
    "oulu": (65.01, 25.47, "FI"), "rovaniemi": (66.50, 25.73, "FI"), "kuopio": (62.89, 27.68, "FI"),
    "oslo": (59.91, 10.75, "NO"), "bergen": (60.39, 5.32, "NO"), "trondheim": (63.43, 10.40, "NO"),
    "stavanger": (58.97, 5.73, "NO"), "tromso": (69.65, 18.96, "NO"), "bodo": (67.28, 14.40, "NO"),
}

CURRENT = StructType([
    StructField("time", StringType()),
    StructField("temperature_2m", DoubleType()),
    StructField("precipitation", DoubleType()),
    StructField("rain", DoubleType()),
    StructField("snowfall", DoubleType()),
    StructField("wind_speed_10m", DoubleType()),
    StructField("wind_gusts_10m", DoubleType()),
    StructField("weather_code", IntegerType()),
    StructField("cloud_cover", DoubleType()),
    StructField("visibility", DoubleType()),
])
SCHEMA = StructType([
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("current", CURRENT),
])


def nearest_city(lat: Column, lon: Column) -> Column:
    """Struct(dist2, id, country) of the closest city; plain column expressions, no Python UDF."""
    candidates = [
        F.struct(((lat - c_lat) ** 2 + (lon - c_lon) ** 2).alias("d"),
                 F.lit(cid).alias("id"), F.lit(country).alias("country"))
        for cid, (c_lat, c_lon, country) in CITIES.items()
    ]
    return F.array_min(F.array(*candidates))   # structs compare by their first field


def parse(frame: DataFrame) -> DataFrame:
    w = F.col("w")
    c = w["current"]
    city = nearest_city(w["latitude"], w["longitude"])
    out = frame.select(F.from_json("value", SCHEMA).alias("w"), "ingest_time").select(
        city["id"].alias("location_id"),
        city["country"].alias("country"),
        w["latitude"].alias("latitude"),
        w["longitude"].alias("longitude"),
        # "2026-11-25T21:30" in GMT (the request sets timezone=GMT)
        F.to_timestamp(c["time"], "yyyy-MM-dd'T'HH:mm").alias("event_time"),
        c["temperature_2m"].alias("temperature_c"),
        c["precipitation"].alias("precipitation_mm"),
        c["rain"].alias("rain_mm"),
        c["snowfall"].alias("snowfall_cm"),
        c["wind_speed_10m"].alias("wind_speed_kmh"),
        c["wind_gusts_10m"].alias("wind_gust_kmh"),
        c["weather_code"].alias("weather_code"),
        c["cloud_cover"].alias("cloud_cover_pct"),
        c["visibility"].alias("visibility_m"),
        F.col("ingest_time"),
    )
    return out.where(F.col("event_time").isNotNull() & F.col("latitude").isNotNull()) \
        .withColumn("dt", utc_date("event_time"))


SOURCE = Source(
    name="weather",
    topic="weather",
    table="weather_observations",
    parse=parse,
    dedup_keys=("location_id", "event_time"),
    dedup_watermark="2 hours",
    description="Current weather in 12 Finnish and Norwegian cities (Open-Meteo, every 15 min)",
    quality_rules={
        "location_id": {"not_null": True, "allowed": sorted(CITIES)},
        "temperature_c": {"not_null": True, "range": (-50.0, 40.0)},
        "precipitation_mm": {"range": (0.0, 100.0)},
        "wind_speed_kmh": {"range": (0.0, 200.0)},
        "visibility_m": {"range": (0.0, 100000.0)},
        "event_time": {"not_null": True, "max_lag_seconds": 3600},
    },
)
