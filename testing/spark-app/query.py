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
