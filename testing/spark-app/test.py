import sys
import os
from random import random
from operator import add

from pyspark.sql import SparkSession

os.environ["SPARK_LOCAL_IP"] = "129.74.152.201"

if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    spark = (
        SparkSession.builder.appName("PythonPi")
        .master("spark://129.74.152.201:7077")
        .config("spark.blockManager.port", "10025")
        .config("spark.driver.blockManager.port", "10026")
        .config("spark.driver.port", "10027")
        .config("spark.driver.host", "129.74.152.201")
        .getOrCreate()
    )

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 1000000 * partitions

    def f(_: int) -> float:
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x**2 + y**2 <= 1 else 0

    count = (
        spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    )
    print("Pi is roughly %f" % (4.0 * count / n))

    spark.stop()
