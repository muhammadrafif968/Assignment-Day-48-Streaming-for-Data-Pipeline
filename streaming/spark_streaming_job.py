# spark_streaming_job.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import from_json, col, when, window, sum as _sum, current_timestamp, expr

spark = SparkSession.builder.appName("TransactionStreaming").getOrCreate()

kafka_bootstrap = "kafka:9092"
input_topic = "transactions"
valid_topic = "transactions_valid"
dlq_topic = "transactions_dlq"

# Schema JSON
schema = StructType()\
    .add("user_id", StringType())\
    .add("amount", IntegerType())\
    .add("timestamp", StringType())\
    .add("source", StringType())

# Read Kafka
raw_df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", kafka_bootstrap)\
    .option("subscribe", input_topic)\
    .option("startingOffsets", "earliest")\
    .load()

df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
df = df.withColumn("event_time", expr("to_timestamp(timestamp)"))

valid_sources = ["mobile","web","pos"]

# Validasi
df_validated = df.withColumn("is_valid",
    when(col("user_id").isNull(), False)
    .when(col("amount").isNull(), False)
    .when(col("timestamp").isNull(), False)
    .when((col("amount")<1) | (col("amount")>10000000), False)
    .when(~col("source").isin(valid_sources), False)
    .otherwise(True)
).withColumn("error_reason",
    when(col("user_id").isNull(), "missing_user_id")
    .when(col("amount").isNull(), "missing_amount")
    .when(col("timestamp").isNull(), "missing_timestamp")
    .when((col("amount")<1) | (col("amount")>10000000), "invalid_amount")
    .when(~col("source").isin(valid_sources), "invalid_source")
    .otherwise(None)
)

# Duplicate detection
df_validated = df_validated.dropDuplicates(["user_id","event_time"])

# Watermark 3 menit
df_watermarked = df_validated.withWatermark("event_time","3 minutes")

# Split valid / invalid
df_valid = df_watermarked.filter(col("is_valid")==True)
df_invalid = df_watermarked.filter(col("is_valid")==False)

# Write valid to Kafka
df_valid.selectExpr("to_json(struct(*)) AS value")\
    .writeStream.format("kafka")\
    .option("kafka.bootstrap.servers", kafka_bootstrap)\
    .option("topic", valid_topic)\
    .option("checkpointLocation","/tmp/checkpoint_valid")\
    .start()

# Write invalid to DLQ
df_invalid.selectExpr("to_json(struct(*)) AS value")\
    .writeStream.format("kafka")\
    .option("kafka.bootstrap.servers", kafka_bootstrap)\
    .option("topic", dlq_topic)\
    .option("checkpointLocation","/tmp/checkpoint_dlq")\
    .start()

# Tumbling window 1 menit
windowed_df = df_valid.groupBy(window("event_time","1 minute"))\
    .agg(_sum("amount").alias("running_total"))\
    .withColumn("timestamp", current_timestamp())

windowed_df.writeStream.outputMode("complete").format("console").option("truncate",False).start()
spark.streams.awaitAnyTermination()