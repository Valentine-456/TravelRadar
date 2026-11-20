from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.appName("ExampleJob").getOrCreate()

df = spark.createDataFrame([("1",)], ["seq"]).withColumn("now", current_timestamp())
df.show(truncate=False)

spark.stop()
