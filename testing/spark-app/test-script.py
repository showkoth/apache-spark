import pyspark.sql as ps

spark = ps.SparkSession.builder \
    .appName("TestCsv") \
    .master("local") \
    .getOrCreate()

input_path = "/opt/spark/work-dir/tips.csv"
output_path= "/opt/spark/work-dir/output.csv"

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