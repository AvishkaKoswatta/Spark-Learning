from pyspark import SparkConf, SparkContext

def partitioning_example():
    conf = SparkConf().setAppName("RDDPartitioning").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # -----------------------------
    # Load the orders dataset
    # -----------------------------
    orders_path = "/home/avishka/data/projects/spark/data/orders.csv"
    lines = sc.textFile(orders_path)
    header = lines.first()
    data = lines.filter(lambda row: row != header)

    print(f"Initial number of partitions: {data.getNumPartitions()}")

    # -----------------------------
    # See how data is distributed
    # -----------------------------
    def show_partition_index(index, iterator):
        yield (index, list(iterator)[:2])  # show first 2 elements per partition

    print("\nSample records per partition (before repartition):")
    for part in data.mapPartitionsWithIndex(show_partition_index).collect():
        print(part)

    # -----------------------------
    # Change partitions
    # -----------------------------
    repartitioned = data.repartition(4)
    print(f"\nAfter repartition(4): {repartitioned.getNumPartitions()}")

    coalesced = repartitioned.coalesce(2)
    print(f"After coalesce(2): {coalesced.getNumPartitions()}")

    # -----------------------------
    # Partitioning with Key-Value data
    # -----------------------------
    pairs = data.map(lambda line: (line.split(",")[1], line))
    print(f"\nInitial partitioner: {pairs.partitioner}")

    # Repartition by key (hash partitioner)
    partitioned_by_key = pairs.partitionBy(3)
    print(f"After partitionBy(3): {partitioned_by_key.getNumPartitions()}")

    # Show which partition some keys go to
    print("\nSample of keys and their partition assignment:")
    for (key, _) in partitioned_by_key.mapPartitionsWithIndex(
        lambda index, it: [(index, [k for k, _ in list(it)[:2]])]
    ).collect():
        print(f"Partition {key}: {_[0:2]}")

    sc.stop()


if __name__ == "__main__":
    partitioning_example()
