import sys
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Build-DB")
    .master("spark://129.74.152.201:7077")
    .config("spark.blockManager.port", "10025")
    .config("spark.driver.blockManager.port", "10026")
    .config("spark.driver.port", "10027")
    .config("spark.driver.host", "129.74.152.201")
    .config("spark.hadoop.fs.defaultFS", "hdfs://129.74.152.201:9000")
    .config("spark.sql.warehouse.dir", "hdfs://129.74.152.201:9000/data/warehouse")
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


# get db info
spark.sql(f"SELECT COUNT(*) FROM {db_name}.{table_name}").show()
spark.sql(f"DESCRIBE {db_name}.{table_name}").show()

df = spark.sql(f"SELECT * FROM {db_name}.{table_name} WHERE job = 'writer'")
df.show()
spark.stop()