import time

import redis
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType
from kafka_topics import KafkaTopics

REDIS_HOST = "redis"
RETENTION_SECONDS = 24 * 3600


spark: SparkSession = SparkSession.builder.appName("OpenSky").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", KafkaTopics.OpenSky.value) \
    .option("startingOffsets", "earliest") \
    .load()

df2 = df.select(
    F.date_trunc("minute", "timestamp").alias("timestamp"), 
    F.from_json(F.col("value").cast("string"), ArrayType(StringType())).alias("json")
)

df3 = df2.withColumn("icao24", F.col("json")[0]) \
    .withColumn("callsign", F.trim(F.col("json")[1])) \
    .withColumn("origin_country", F.col("json")[2]) \
    .withColumn("longitude", F.col("json")[5].cast("double")) \
    .withColumn("latitude", F.col("json")[6].cast("double")) \
    .withColumn("velocity", F.col("json")[9].cast("double")) \
    .withColumn("true_track", F.col("json")[10].cast("double")) \
    .withColumn("altitude", F.coalesce(F.col("json")[13], F.col("json")[7]).cast("double")) \
    .withColumn("minute", F.col("timestamp").cast("long")) \
    .dropna(subset=["latitude", "longitude", "true_track"]) \
    .drop("json")


def write_partition(rows) -> None:
    r = redis.Redis(host=REDIS_HOST, port=6379)
    pipe = r.pipeline()
    for row in rows:
        key = f"planes:snap:{row.minute}"
        pipe.hset(key, row.icao24, row.data)
        pipe.expireat(key, row.minute + RETENTION_SECONDS)
        pipe.zadd("planes:snaps", {str(row.minute): row.minute})
    pipe.execute()


def write_batch(batch_df, batch_id: int) -> None:
    planes = batch_df.dropDuplicates(["minute", "icao24"]).select(
        "minute",
        "icao24",
        F.to_json(F.struct(
            "icao24", "callsign", "origin_country",
            "longitude", "latitude", "altitude", "velocity", "true_track",
        )).alias("data"),
    )
    planes.foreachPartition(write_partition)
    redis.Redis(host=REDIS_HOST, port=6379).zremrangebyscore(
        "planes:snaps", "-inf", time.time() - RETENTION_SECONDS
    )
    print(f"batch {batch_id} written to Redis")


query = df3.writeStream \
    .foreachBatch(write_batch) \
    .option("checkpointLocation", "/tmp/checkpoints/opensky") \
    .trigger(processingTime="10 seconds") \
    .start()
query.awaitTermination()

