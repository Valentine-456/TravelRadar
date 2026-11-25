from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    spark = (SparkSession.builder.appName(app_name)
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             # keep partition values such as dt=2026-11-25 as strings, like the Hive tables
             .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
