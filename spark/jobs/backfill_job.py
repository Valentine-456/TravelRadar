"""Batch acquisition of historical data (Lambda batch layer input).

    ./submit.sh backfill_job.py rail --days 60                       # last 60 complete days
    ./submit.sh backfill_job.py rail --start 2026-10-01 --end 2026-10-31
    ./submit.sh backfill_job.py weather --start 2025-01-01 --end 2026-11-24

rail     Digitraffic archive, one request per day (https://rata.digitraffic.fi/api/v1/trains/<date>),
         final state of every train of that day.
weather  Open-Meteo archive, hourly values for the 12 cities of the live weather feed.

Each day is written as raw records (same schema as the streaming raw archive) and as cleaned rows
produced by the *same* parse function as the live stream. Days are overwritten, so re-running a
range is idempotent and fills gaps left by failed downloads.
"""
from __future__ import annotations

import argparse
import gzip
import json
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from travelradar import config
from travelradar.session import get_spark
from travelradar.sources import digitraffic_trains, weather

USER_AGENT = "TravelRadar student project (Warsaw University of Technology)"
FRAME_SCHEMA = StructType([StructField("value", StringType()),
                           StructField("ingest_time", TimestampType())])

HOURLY = ["temperature_2m", "precipitation", "rain", "snowfall", "wind_speed_10m",
          "wind_gusts_10m", "weather_code", "cloud_cover"]


def http_json(url: str, headers: dict | None = None, retries: int = 4):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept-Encoding": "gzip",
                                               **(headers or {})})
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                body = resp.read()
                if resp.headers.get("Content-Encoding") == "gzip":
                    body = gzip.decompress(body)
                return json.loads(body)
        except Exception as exc:  # network errors and 429/5xx: back off, then give up
            if attempt == retries:
                raise RuntimeError(f"GET {url} failed: {exc}") from exc
            time.sleep(2 ** attempt * 5)


def to_frame(spark: SparkSession, values: list[str]) -> DataFrame:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return spark.createDataFrame([(v, now) for v in values], FRAME_SCHEMA)


def write_day(frame: DataFrame, cleaned: DataFrame, name: str, table: str, day: date) -> int:
    """Raw and cleaned rows of one day; overwrites only that day's partition."""
    raw = frame.withColumn("dt", F.lit(day.isoformat()))
    raw.coalesce(1).write.mode("overwrite").partitionBy("dt") \
        .parquet(f"{config.BATCH_ROOT}/raw/source={name}")
    cleaned = cleaned.withColumn("dt", F.lit(day.isoformat()))
    cleaned.coalesce(1).write.mode("overwrite").partitionBy("dt") \
        .parquet(f"{config.BATCH_ROOT}/{table}")
    return cleaned.count()


# ---- rail -------------------------------------------------------------------------------------

def rail_day_frame(spark: SparkSession, trains: list[dict]) -> DataFrame:
    return to_frame(spark, [json.dumps(t, separators=(",", ":")) for t in trains])


def rail_cleaned(frame: DataFrame) -> DataFrame:
    src = digitraffic_trains.SOURCE
    # the archive holds one final version per train, so plain de-duplication is enough
    return src.parse(frame).dropDuplicates(list(src.dedup_keys)).drop("dt")


def backfill_rail(spark: SparkSession, days: list[date]) -> None:
    for day in days:
        url = f"https://rata.digitraffic.fi/api/v1/trains/{day.isoformat()}"
        try:
            trains = http_json(url, {"Digitraffic-User": "travelradar-student-project"})
        except RuntimeError as exc:
            print(f"{day}: skipped ({exc}); re-run the range to fill the gap", flush=True)
            continue
        frame = rail_day_frame(spark, trains)
        n = write_day(frame, rail_cleaned(frame), "digitraffic_trains_archive",
                      "train_stop_events_history", day)
        print(f"{day}: {len(trains)} trains, {n} stop events", flush=True)
        time.sleep(1)  # be polite to the open API


# ---- weather ----------------------------------------------------------------------------------

def weather_values(response: list[dict] | dict) -> list[str]:
    """Hourly archive arrays -> one record per city and hour, shaped like a live `current` record,
    so weather.parse handles both."""
    entries = response if isinstance(response, list) else [response]
    out = []
    for entry in entries:
        hourly = entry["hourly"]
        for i, t in enumerate(hourly["time"]):
            current = {"time": t, **{k: hourly[k][i] for k in HOURLY if k in hourly}}
            out.append(json.dumps({"latitude": entry["latitude"], "longitude": entry["longitude"],
                                   "current": current}, separators=(",", ":")))
    return out


def backfill_weather(spark: SparkSession, start: date, end: date) -> None:
    cities = weather.CITIES.values()
    params = {
        "latitude": ",".join(str(c[0]) for c in cities),
        "longitude": ",".join(str(c[1]) for c in cities),
        "start_date": start.isoformat(), "end_date": end.isoformat(),
        "hourly": ",".join(HOURLY), "timezone": "GMT",
    }
    url = "https://archive-api.open-meteo.com/v1/archive?" + urllib.parse.urlencode(params)
    frame = to_frame(spark, weather_values(http_json(url)))
    cleaned = weather.SOURCE.parse(frame).dropDuplicates(list(weather.SOURCE.dedup_keys))
    frame.withColumn("dt", F.lit(end.isoformat())).coalesce(1).write.mode("overwrite") \
        .partitionBy("dt").parquet(f"{config.BATCH_ROOT}/raw/source=open_meteo_archive")
    cleaned.coalesce(4).write.mode("overwrite").partitionBy("dt") \
        .parquet(f"{config.BATCH_ROOT}/weather_history")
    print(f"weather {start}..{end}: {cleaned.count()} hourly observations", flush=True)


# ---- cli --------------------------------------------------------------------------------------

def day_range(args) -> list[date]:
    if args.days:
        end = datetime.now(timezone.utc).date() - timedelta(days=1)
        start = end - timedelta(days=args.days - 1)
    else:
        start, end = args.start, args.end
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("dataset", choices=["rail", "weather"])
    ap.add_argument("--days", type=int, help="last N complete days (UTC)")
    ap.add_argument("--start", type=date.fromisoformat)
    ap.add_argument("--end", type=date.fromisoformat)
    args = ap.parse_args()
    if not args.days and not (args.start and args.end):
        ap.error("give --days N, or --start and --end")
    days = day_range(args)

    spark = get_spark(f"Backfill-{args.dataset}")
    if args.dataset == "rail":
        backfill_rail(spark, days)
    else:
        backfill_weather(spark, days[0], days[-1])
    spark.stop()


if __name__ == "__main__":
    main()
