from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.6,software.amazon.awssdk:bundle:2.33.13").appName("CSV to Parquet").getOrCreate()

df = spark.read.csv("s3a://test/test.csv", header=True, inferSchema=True)
df.write.mode("overwrite").parquet("s3a://test/test.parquet")