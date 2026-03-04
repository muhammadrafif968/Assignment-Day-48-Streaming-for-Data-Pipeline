from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder \
    .appName("KafkaStreamingJob") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:19092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Show raw data to console
query = df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()