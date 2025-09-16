from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CSV to Parquet").getOrCreate()

df = spark.read.csv("s3a://test/test.csv", header=True, inferSchema=True)
df.write.mode("overwrite").parquet("s3a://test/test.parquet")