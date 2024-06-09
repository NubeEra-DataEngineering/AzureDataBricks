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
