from pyspark.sql import SparkSession
from pyspark.sql.functions import rand

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("LongRunningTask") \
    .master("spark://8.tcp.ngrok.io:15492") \
    .config("spark.executor.memory", "1G") \
    .config("spark.executor.cores", "1") \
    .getOrCreate()

sc = spark.sparkContext

# Number of total points
NUM_SAMPLES = 100000000  

def inside_circle(_):
    """Check if a randomly generated point falls inside a unit circle."""
    x, y = rand().eval(), rand().eval()  # Random point
    return 1 if x*x + y*y <= 1 else 0

# Parallelize the workload across 3 workers
rdd = sc.parallelize(range(NUM_SAMPLES), numSlices=3*2)  # 3 workers * 2 cores each

# Compute Pi using Monte Carlo method
inside_count = rdd.map(inside_circle).reduce(lambda a, b: a + b)
pi_estimate = (4.0 * inside_count) / NUM_SAMPLES

print(f"Estimated Pi: {pi_estimate}")

# Stop Spark Session
spark.stop