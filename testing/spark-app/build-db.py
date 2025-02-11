import sys
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Build-DB")
    .master("local[6]")
    .config("spark.executor.memory", "2g")
    .config("spark.sql.shuffle.partitions", "2")  # Reduce for small datasets
    .config("spark.default.parallelism", "2")
    .config("spark.driver.bindAddress", "localhost")
    .config("spark.driver.host", "localhost")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "1g")
    .enableHiveSupport()
    .getOrCreate()
)


tsv_file = sys.argv[1]
table_name = "principals_table"

db_name = "default"

print("Reading in data")
df = spark.read.option("header", "true").option("delimiter", "\t").csv(tsv_file)
print("Fiished reading data")
df.printSchema()

print("Writing Data")
df.write.mode("overwrite").saveAsTable(f"{db_name}.{table_name}")

print("Data successfully written to table")

spark.stop()
