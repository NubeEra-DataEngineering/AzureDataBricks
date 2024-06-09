# Import necessary libraries
from pyspark.sql.functions import *

# Set the input path (use a sample dataset for demonstration)
input_path = "/databricks-datasets/structured-streaming/events/"

# Set the checkpoint location
checkpoint_location = "/mnt/data/checkpoints/"

# Read from the input path using Autoloader
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load(input_path)
)

# Process the data (for example, count events by event type)
processed_df = df.groupBy("eventType").count()

# Write the output to a Delta table with checkpointing enabled
output_path = "/mnt/data/output/"

query = (
    processed_df.writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", checkpoint_location)
    .start(output_path)
)
