import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, lit, row_number
from pyspark.sql.window import Window

# Set environment variables for local mode
os.environ["SPARK_LOCAL_IP"] = "localhost"
os.environ["SPARK_WORKER_CORES"] = "2"  # Adjust based on your machine
os.environ["SPARK_WORKER_MEMORY"] = "2g"  # Adjust based on your machine

spark = (
    SparkSession.builder.appName("Web3DB")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .config("spark.sql.shuffle.partitions", "2")  # Reduce for small datasets
    .config("spark.default.parallelism", "2")
    .config("spark.blockManager.port", "10025")
    .config("spark.driver.blockManager.port", "10026")
    .config("spark.driver.port", "10027")
    .config("spark.driver.host", "172.23.153.11")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "1g")
    .config("spark.sql.warehouse.dir", "./spark-warehouse")
    .enableHiveSupport()
    .getOrCreate()
)

db_name = "default"
table_name = "principals_table"

spark.sql("SHOW DATABASES").show()
spark.sql("USE default")
spark.sql("SHOW TABLES").show()

# get db info
spark.sql(f"SELECT COUNT(*) FROM {db_name}.{table_name}").show()
spark.sql(f"DESCRIBE {db_name}.{table_name}").show()

df = spark.sql(f"SELECT * FROM {db_name}.{table_name} WHERE job = 'writer'")
df.show()
print(df.count())
exit()
print("Creating employee table:")

spark.sql("""
        CREATE TABLE IF NOT EXISTS employees (
            id INT, 
            name STRING,
            salary FLOAT
        )
""")

spark.sql("SHOW TABLES").show()
print("Finished creating table")


# generate random data
print("Generating random data")
num_rows = 200000000


df = spark.range(num_rows).select(
    row_number().over(Window.orderBy(lit(1))).alias("id"),
    (rand() * 1000000000).cast("int").alias("name"),
    (rand() * 100000).cast("float").alias("salary"),
)
df.show()
print("Random Data generation complete.")

df.write.insertInto("employees", overwrite=True)

spark.sql("SELECT COUNT (*) FROM employees").show()
spark.sql("SELECT * FROM employees LIMIT 10").show()
