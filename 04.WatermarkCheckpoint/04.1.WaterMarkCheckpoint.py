# Databricks notebook source
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, window
from pyspark.sql.types import StructType, StringType, TimestampType

# COMMAND ----------

# # Initialize Spark session
# spark = SparkSession.builder.appName("CheckpointingDemo").getOrCreate()

# Define the schema for the events data
schema = StructType() \
    .add("date", TimestampType()) \
    .add("event_type", StringType()) \
    .add("event_message", StringType())

# Load sample dataset from databricks-datasets
input_path = "/databricks-datasets/structured-streaming/events/"

# COMMAND ----------

# MAGIC %md
# MAGIC Structured Streaming uses watermarks to control the threshold for how long to continue processing updates for a given state entity.
# MAGIC - Aggregations over a time windows
# MAGIC - Unique keys in a join between two streams

# COMMAND ----------

# MAGIC %md
# MAGIC # Why Checkpoint over watermark
# MAGIC - Structured Streaming provides fault-tolerance and data consistency for streaming queries; using Azure Databricks workflows, 
# MAGIC - you can easily configure your Structured Streaming queries to automatically restart on failure. 
# MAGIC - By enabling checkpointing for a streaming query, you can restart the query after a failure.
# MAGIC - The restarted query continues where the failed one left off.

# COMMAND ----------

# Read the input data stream
input_stream = spark.readStream \
    .schema(schema) \
    .json(input_path)

# Apply watermarking and transformation
watermarked_stream = input_stream \
    .withWatermark("date", "1 hour") \
    .withColumn("event_date", expr("date_trunc('day', date)")) \
    .groupBy(window("date", "1 day"), "event_type") \
    .count()

# Define the output path and checkpoint location
output_path = "/tmp/output/checkpointing_demo"
checkpoint_location = "/tmp/checkpoints/checkpointing_demo"

# Write the transformed stream to the output with checkpointing
query = watermarked_stream.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_location) \
    .start()

# COMMAND ----------

# Wait for the termination of the query
query.awaitTermination()

# COMMAND ----------

# Reading the output data to verify
output_df = spark.read.parquet(output_path)
display(output_df)

# COMMAND ----------

# Stopping the Spark session
spark.stop()

# COMMAND ----------

# Reading the output data to verify
output_df = spark.read.parquet(output_path)
display(output_df)

# COMMAND ----------

# Stopping the Spark session
spark.stop()
