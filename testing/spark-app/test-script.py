import pyspark.sql as ps

spark = ps.SparkSession.builder \
    .appName("TestCsv") \
    .master("local") \
    .getOrCreate()

input_path = "/opt/spark/work-dir/test.csv"
output_path= "/opt/spark/work-dir/output.csv"

df = spark.read\
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

print("Input Data")
df.show()

new_df = df.filter(ps.functions.col("HUCENSUS2010") > 10000).select("STATE", "CTYNAME")
print("Transformed Data")
new_df.show()

new_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

spark.stop()