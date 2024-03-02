import pyspark
from pyspark.sql import SparkSession

from delta import *

builder = pyspark.sql.SparkSession.builder.appName("DeltaTutorial") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
# data = spark.range(100, 105)
# data.write.format("delta").save("/tmp/delta-table")
# data = spark.range(100).repartition(2)
# data.write.format("delta").save("/tmp/test-2/")

spark.read.format("delta").load("/tmp/delta-table/").show()