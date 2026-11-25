"""One-shot smoke test: write and read back a tiny Parquet dataset on HDFS (no Kafka needed).

    ./submit.sh hdfs_smoke_write.py
"""
from pyspark.sql import functions as F

from travelradar import config
from travelradar.session import get_spark

SMOKE_PATH = f"{config.HDFS_ROOT}/views/_smoke"

spark = get_spark("HdfsSmokeWrite")
df = spark.range(0, 10).withColumn("label", F.concat(F.lit("row-"), F.col("id"))) \
    .withColumn("dt", F.lit("1970-01-01"))
df.coalesce(1).write.mode("overwrite").partitionBy("dt").parquet(SMOKE_PATH)
count = spark.read.parquet(SMOKE_PATH).count()
print(f"wrote 10 rows to {SMOKE_PATH}, read back {count}")
if count != 10:
    raise SystemExit(f"expected 10 rows, got {count}")
print("HDFS smoke test OK")
spark.stop()
