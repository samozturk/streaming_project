from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType


PROJECT_ID = "our-bebop-431708-i3"
DATASET_ID = "bcn"
TABLE_ID = "revised_raw"

# Write to BigQuery
def write_to_bigquery(batch_df, batch_id):
    batch_df.write \
        .format("bigquery") \
        .option("table", f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}") \
        .option("writeMethod", "direct") \
        .mode("append") \
        .save()
    print(f"Batch {batch_id} written to BigQuery")


# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2") \
    .config("spark.jars", "./spark-bigquery-latest_2.12.jar") \
    .getOrCreate()

# Define the schema of your data
schema = StructType([
    StructField("time_ref", DateType(), True),
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
# parsed_df = df.select(
#     from_json(col("value").cast("string"), schema).alias("data")
# ).select("data.*")
# Deserialize JSON data
parsed_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

parsed_df = parsed_df.withColumn("value", col("value").cast("double"))

# Process the data as needed
# For example, you can simply show the data:
query = parsed_df \
    .writeStream \
    .queryName("Kafka Read a.k.a Sirlar Stream") \
    .outputMode("append") \
    .format("console") \
    .start()
# or write to bigquery
# query = parsed_df \
#     .writeStream \
#     .queryName("Kafka Read a.k.a Sirlar Stream") \
#     .foreachBatch(write_to_bigquery) \
#     .outputMode("append") \
#     .start()

query.awaitTermination()

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 /data/scripts/job.py
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2 job.py
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 --jars ./spark-3.5-bigquery-0.41.0.jar /data/scripts/job.py
