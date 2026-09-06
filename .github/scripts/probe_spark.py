"""A two-line Spark job, enough to produce application, job, stage and task events."""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("convalesce_probe").getOrCreate()
rows = spark.range(0, 1000).filter("id % 7 == 0").count()
print("PROBE_ROWS", rows, flush=True)
spark.stop()
