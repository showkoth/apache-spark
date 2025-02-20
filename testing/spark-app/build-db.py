import sys
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("PythonPi")
    .master("spark://129.74.152.201:7077")
    .config("spark.blockManager.port", "10025")
    .config("spark.driver.blockManager.port", "10026")
    .config("spark.driver.port", "10027")
    .config("spark.driver.host", "129.74.152.201")
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

spark.sql("SHOW TABLES").show()
print("Data successfully written to table")

spark.stop()
