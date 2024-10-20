from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Define the schema of your data
schema = StructType([
    StructField("time_ref", StringType(), True),
    StructField("account", StringType(), True),
    StructField("code", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("product_type", StringType(), True),
    StructField("value", StringType(), True),
    StructField("status", StringType(), True)
])

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "csvdatatopic") \
    .load()

# Parse the value column from Kafka
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Process the data as needed
# For example, you can simply show the data:
query = parsed_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query = parsed_df.select("time_ref", "account", "value") \
    .limit(5) \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()


