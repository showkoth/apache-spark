import os
import pyspark.sql as ps


os.environ['SPARK_LOCAL_IP'] = 'localhost'

spark = (ps.SparkSession.builder
    .appName("TestCsv")
    .master("spark://6.tcp.ngrok.io:17983")
    .config("spark.driver.memory", "1g")
    .config("spark.executor.memory", "1g")
    .config("spark.sql.shuffle.partitions", "2")  # Reduce for small datasets
    .config("spark.default.parallelism", "2")
    .config("spark.driver.bindAddress", "localhost")
    .config("spark.driver.host", "localhost")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "1g")
    .getOrCreate()
)

possible_paths = ["/home/rnahm/web3db/apache-spark/testing/spark-app/tips.csv", "/opt/spark/work-dir/tips.csv"]

input_path = next((path for path in possible_paths if os.path.exists(path)), None)
output_path= "output.csv"

df = spark.read\
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

print("Input Data")
df.show()

new_df = df.filter(ps.functions.col("tip") > 2).select("sex", "time")
print("Transformed Data")
new_df.show()

# new_df.write \
#     .mode("overwrite") \
#     .option("header", "true") \
#     .csv(output_path)

spark.stop()