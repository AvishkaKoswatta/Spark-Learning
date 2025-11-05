from pyspark import SparkConf, SparkContext
import re

# -------------------------------
# Initialize Spark
# -------------------------------
conf = SparkConf().setAppName("AdvancedLogProcessingRDD").setMaster("local[*]")
sc = SparkContext(conf=conf)

# -------------------------------
# Load unstructured log data
# -------------------------------
log_rdd = sc.textFile("data/logs.txt")

print("\n--- SAMPLE LOGS ---")
for line in log_rdd.take(5)
    print(line)