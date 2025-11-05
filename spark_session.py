from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkLearning") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

print("Spark session created!")
