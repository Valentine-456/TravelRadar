import time

import redis
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from kafka_topics import KafkaTopics
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType

REDIS_HOST = "redis"
RETENTION_SECONDS = 24 * 3600
BUCKET_SECONDS = 5 * 60  # one snapshot per 5 minutes, ships move slowly


spark: SparkSession = SparkSession.builder.appName("AISStream").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", KafkaTopics.AISStream.value) \
    .option("startingOffsets", "earliest") \
    .load()


schema = StructType([
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
        ])),
    ])),
])

parsed = df.select(
    "timestamp",
    F.from_json(F.col("value").cast("string"), schema).alias("m"),
).where(F.col("m.MessageType") == "PositionReport")

report = F.col("m.Message.PositionReport")

ships = parsed.select(
    (F.floor(F.col("timestamp").cast("long") / BUCKET_SECONDS) * BUCKET_SECONDS).cast("long").alias("bucket"),
    F.col("timestamp").alias("event_time"),
    F.col("m.MetaData.MMSI").alias("mmsi"),
    F.trim(F.col("m.MetaData.ShipName")).alias("ship_name"),
    report["Latitude"].alias("latitude"),
    report["Longitude"].alias("longitude"),
    report["Sog"].alias("speed_knots"),
    # TrueHeading 511 = not available -> fall back to course over ground (360 = not available)
    F.when(report["TrueHeading"] != 511, report["TrueHeading"].cast("double"))
     .when(report["Cog"] < 360, report["Cog"])
     .alias("heading"),
    report["NavigationalStatus"].alias("nav_status"),
).where(
    # AIS uses 91 / 181 for "position not available"
    F.col("mmsi").isNotNull()
    & F.col("latitude").between(-90, 90)
    & F.col("longitude").between(-180, 180)
)


def write_partition(rows) -> None:
    r = redis.Redis(host=REDIS_HOST, port=6379)
    pipe = r.pipeline()
    for row in rows:
        key = f"ships:snap:{row.bucket}"
        pipe.hset(key, str(row.mmsi), row.data)
        pipe.expireat(key, row.bucket + RETENTION_SECONDS)
        pipe.zadd("ships:snaps", {str(row.bucket): row.bucket})
    pipe.execute()


def write_batch(batch_df, batch_id: int) -> None:
    latest = batch_df.groupBy("bucket", "mmsi").agg(
        # newest position of each ship within the bucket
        F.max_by(
            F.to_json(F.struct(
                "mmsi", "ship_name", "latitude", "longitude",
                "speed_knots", "heading", "nav_status",
            )),
            "event_time",
        ).alias("data"),
    )
    latest.foreachPartition(write_partition)
    redis.Redis(host=REDIS_HOST, port=6379).zremrangebyscore(
        "ships:snaps", "-inf", time.time() - RETENTION_SECONDS
    )
    print(f"batch {batch_id} written to Redis")


query = ships.writeStream \
    .foreachBatch(write_batch) \
    .option("checkpointLocation", "/tmp/checkpoints/aisstream") \
    .trigger(processingTime="10 seconds") \
    .start()
query.awaitTermination()
