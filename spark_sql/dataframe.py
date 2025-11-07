from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, split, sum as _sum, avg


spark = SparkSession.builder \
    .appName("OrdersDataframeBasicsExample") \
    .master("local[*]") \
    .getOrCreate()

orders_df = spark.read.csv(
    "/home/avishka/data/projects/spark/data/df_orders.csv",
    header=True,
    inferSchema=True
)

print("Sample data:")
orders_df.show(5)
print("Schema:")
orders_df.printSchema()

print(f"Total records: {orders_df.count()}")

# # Filter by category
# print("Clothing Orders:")
# clothing_df = orders_df.filter(col("category") == "Clothing")
# print(f"Clothing category records: {clothing_df.count()}")
# clothing_df.show(3)

# # Aggregation
# print("Total amount spent by a customer")
# total_spent_df = orders_df.groupBy("user_id").agg(sum("total_price").alias("total_spent"))
# ordered_total_spent_df =total_spent_df.orderBy("total_spent", descending=True)
# ordered_total_spent_df.show(5)



clean_df = (
    orders_df.dropna()
             .dropDuplicates()
             .withColumn("customer_segmentation", split(col("customer_segment"), "-")[0])
             .drop("customer_segment")
             .filter(col("qty") > 0)
)

summary_df = (
    clean_df.groupBy("category")
            .agg(
                _sum("total_price").alias("total_price"),
                _sum("qty").alias("quantity"),
                avg("price").alias("avg_price")
            )
)
summary_df.show(5)


spark.stop()