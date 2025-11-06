# transformations.py
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("RDD_Transformations").setMaster("local[*]")
sc = SparkContext(conf=conf)

def load_orders(sc):
    # Load CSV file (ignore header)
    lines = sc.textFile("/home/avishka/data/projects/spark/data/orders.csv")
    header = lines.first()
    data = lines.filter(lambda line: line != header)
    return data

def transformations_example():
    
    orders = load_orders(sc)

    # -------------------------------
    # Basic Transformations
    # -------------------------------

    # Split CSV
    split_orders = orders.map(lambda line: line.split(","))

    # Extract columns
    statuses = split_orders.map(lambda cols: cols[2])  # order_status

    # Distinct statuses
    unique_statuses = statuses.distinct()

    # filter delivered orders
    delivered_orders = split_orders.filter(lambda cols: cols[2] == "delivered")

    # Map to (customer_id, 1)
    customer_counts = split_orders.map(lambda cols: (cols[1], 1))

    # Reduce by key (count orders per customer)
    orders_per_customer = customer_counts.reduceByKey(lambda a, b: a + b)

    # Sort by number of orders
    sorted_customers = orders_per_customer.sortBy(lambda x: x[1], ascending=False)

    # FlatMap example (split date/time into words)
    words = orders.flatMap(lambda line: line.split(" "))

    # Return all important RDDs to reuse in actions.py
    return {
        "split_orders": split_orders,
        "unique_statuses": unique_statuses,
        "delivered_orders": delivered_orders,
        "orders_per_customer": orders_per_customer,
        "sorted_customers": sorted_customers,
        "words": words
    }
