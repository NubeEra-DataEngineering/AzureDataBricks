# Databricks notebook source
# Import necessary libraries
from pyspark.sql.functions import *

# Set the input path (use a sample dataset for demonstration)
input_path = "/databricks-datasets/structured-streaming/events/"

# Set the checkpoint location
checkpoint_location = "/mnt/data/checkpoints/"

# Set the schema location
schema_location = "/mnt/data/schema/"


# COMMAND ----------


# Read from the input path using Autoloader
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_location)
    .load(input_path)
)


# COMMAND ----------

display(df)

# COMMAND ----------


# Process the data (for example, count events by event type)
processed_df = df.groupBy("action").count()

# Write the output to a Delta table with checkpointing enabled
output_path = "/mnt/data/output/"

query = (
    processed_df.writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", checkpoint_location)
    .start(output_path)
)


# COMMAND ----------


