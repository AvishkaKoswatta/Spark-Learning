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
for line in log_rdd.take(5):
    print(line)

info_logs = log_rdd.filter(lambda line:"INFO" in line)
error_logs = log_rdd.filter(lambda line: "ERROR" in line)
warn_logs = log_rdd.filter(lambda line: "WARN" in line)

print("\n---Log level counts---")
print("Info: %s" % info_logs.count())
print("Error: %s" % error_logs.count())
print("Warn: %s" % warn_logs.count())

# Flatmap, map, mapreduce
words_rdd = log_rdd.flatMap(lambda line: re.split(r"\W+", line))
filtered_words_rdd = words_rdd.filter(lambda w: not re.match(r"\d{4}-\d{2}-\d{2}", w))
word_pairs = filtered_words_rdd.map(lambda word: (word.lower(), 1))
word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

print("\n---Top 10 words---")
for word, count in word_counts.takeOrdered(10, key=lambda x: -x[1]):
    print("%s: %s" % (word, count))


print("\n---User action count---")
user_rdd=log_rdd.filter(lambda line:"User" in line) #keeps only lines that contain the word “User”
user_actions = user_rdd.map(lambda line: re.search(r"(User\d+)", line).group(1))
user_count_rdd = (
    user_actions.map(lambda user: (user, 1))
                .reduceByKey(lambda a, b: a + b)
)
for user, count in user_count_rdd.collect():
    print(f"{user}: {count} actions")

# Use distinct(), cache(), and take()
distinct_users = user_actions.distinct()
distinct_users.cache()  # example of caching (stores in memory)
print("\nDistinct users:", distinct_users.collect())
print("\nDistinct users count:", distinct_users.count())

sc.stop()