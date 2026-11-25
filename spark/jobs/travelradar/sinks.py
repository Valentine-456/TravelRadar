"""Streaming queries: Kafka -> raw archive (HDFS) and Kafka -> cleaned archive (HDFS).

Both use Spark's Parquet file sink: together with the checkpoint on HDFS it writes every Kafka
record exactly once, also after a crash or restart. They run on a 10-minute trigger so each
micro-batch produces a few reasonably sized files instead of thousands of tiny ones.

The `start_*` functions take an optional `kafka_df` so the unit tests can feed the very same
queries from a file stream instead of Kafka.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery

from travelradar import config
from travelradar.common import Source, kafka_frame, utc_date


def read_kafka(spark: SparkSession, topics: list[str]) -> DataFrame:
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
            .option("subscribe", ",".join(config.TOPIC_PREFIX + t for t in topics))
            .option("startingOffsets", "earliest")
            # Kafka keeps 72 h. If the job was down longer, continue from the oldest offset instead
            # of failing; the gap is visible in the raw archive and the volume view.
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", config.MAX_OFFSETS_PER_TRIGGER)
            .load())


def with_trigger(writer: DataStreamWriter, trigger: str) -> DataStreamWriter:
    if trigger == "availableNow":
        return writer.trigger(availableNow=True)
    return writer.trigger(processingTime=trigger)


def raw_frame(kafka_df: DataFrame) -> DataFrame:
    """Bronze rows: the Kafka record unchanged plus its coordinates (partition, offset)."""
    return kafka_frame(kafka_df).withColumn("dt", utc_date("ingest_time"))


def cleaned_frame(source: Source, frame: DataFrame) -> DataFrame:
    """Silver rows: parsed, validated and de-duplicated within the source's watermark."""
    out = source.parse(frame)
    if source.dedup_keys:
        out = (out.withWatermark("ingest_time", source.dedup_watermark)
               .dropDuplicatesWithinWatermark(list(source.dedup_keys)))
    return out


def _parquet_sink(df: DataFrame, name: str, path: str, partition_by: list[str],
                  trigger: str) -> StreamingQuery:
    writer = (df.coalesce(1).writeStream
              .queryName(name)
              .format("parquet")
              .option("path", path)
              .option("checkpointLocation", config.checkpoint_path(name))
              .partitionBy(*partition_by))
    return with_trigger(writer, trigger).start()


def raw_path(topic: str) -> str:
    # one file sink per topic (a sink owns its folder), laid out as Hive partitions topic=/dt=
    return f"{config.RAW_PATH}/topic={topic}"


def start_raw_archive(spark: SparkSession, source: Source, trigger: str | None = None,
                      kafka_df: DataFrame | None = None) -> StreamingQuery:
    kafka_df = kafka_df if kafka_df is not None else read_kafka(spark, [source.topic])
    return _parquet_sink(raw_frame(kafka_df).drop("topic"), f"raw_{source.name}",
                         raw_path(source.topic), ["dt"], trigger or config.ARCHIVE_TRIGGER)


def start_cleaned_archive(spark: SparkSession, source: Source, trigger: str | None = None,
                          kafka_df: DataFrame | None = None) -> StreamingQuery:
    kafka_df = kafka_df if kafka_df is not None else read_kafka(spark, [source.topic])
    return _parquet_sink(cleaned_frame(source, kafka_frame(kafka_df)), f"cleaned_{source.table}",
                         config.cleaned_path(source.table), ["dt"],
                         trigger or config.ARCHIVE_TRIGGER)
