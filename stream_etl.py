# STREAMING DATA INGESTION
# Modify the ETL pipeline to support real-time data ingestion using Apache Kafka or AWS Kinesis.


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, to_date, sum as _sum, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Spark session initialization
spark = SparkSession.builder.appName("Stream_ETL") \
                            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
                            .getOrCreate()

# We will use Kafka to stream the application real-time, with expectation of 
# zookeeper, topic, comnsumer group and brokers.

# # Subcribe tp a topic
# stream_df = spark \
#            .readStream \
#            .format('kafka') \
#            .option('kafka.bootstrap.servers', 'localhost:9092') \
#            .option('subscribe', 'invoice_topic') \
#            .load()

# # convert kafka message
# stream_df = stream_df.selectExpr("CAST(value AS STRING)")

# Define Kafka topic and broker
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "invoice_topic"

# Define database connection (best practice to use .env , secret manager or store in vault)
DB_URL = "jdbc:postgresql://postgres:5435/chinook_db"
DB_USER = "admin"
DB_PASSWORD = "admin"

# # Define the schema for Kafka JSON messages

schema = StructType([
    StructField("invoice_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Read Kafka stream
stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

# Convert Kafka messages to readable format
json_df = stream_df.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), schema).alias("data")) \
    .select("data.*")
    
# Convert invoice date and aggregate sales by day
stream_df = stream_df.withColumn("InvoiceDate", to_date(col("InvoiceDate")))

# Aggregate streaming sales data by day
daily_sales_stream = stream_df.groupBy(
    window(col("InvoiceDate"), "1 day")
).agg(_sum("Total").alias("DailyRevenue"))

# Write to PostgreSQL using batch writes
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "invoices") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Writing to console
query = stream_df.writeStream \
                 .outputMode('append') \
                 .format('console') \
                 .start()
                 
query.awaitTermination()