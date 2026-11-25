"""One-shot smoke test: write and read a tiny Parquet dataset on HDFS."""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from hdfs_paths import SMOKE_PATH

spark = SparkSession.builder.appName("HdfsSmokeWrite").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.range(0, 10).withColumn("label", F.concat(F.lit("row-"), F.col("id"))) \
    .withColumn("dt", F.lit("1970-01-01")) \
    .withColumn("hour", F.lit("00"))

df.coalesce(1).write.mode("overwrite").partitionBy("dt", "hour").parquet(SMOKE_PATH)
print(f"wrote smoke data to {SMOKE_PATH}")

back = spark.read.parquet(SMOKE_PATH)
count = back.count()
print(f"read-back count={count}")
back.show(truncate=False)

if count != 10:
    raise SystemExit(f"expected 10 rows, got {count}")

print("HDFS smoke test OK")
spark.stop()
