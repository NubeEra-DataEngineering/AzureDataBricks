
# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake & DLT Demo (PySpark)
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC - Introduction to Delta Lake and ACID Transactions
# MAGIC - Creating and Managing Delta Tables
# MAGIC - Time Travel, Schema Enforcement & Evolution
# MAGIC - Batch + Streaming concepts using Auto Loader
# MAGIC - Optimizations with OPTIMIZE, ZORDER, Caching, Compaction
# MAGIC - DLT overview (see separate `dlt/dlt_pipeline.py`)
# MAGIC
# MAGIC **Sample dataset:** https://github.com/NubeEra-Samples/DataSets/blob/master/data.csv
# MAGIC
# MAGIC **Paths & database** are parameterized below.

# COMMAND ----------

# MAGIC %python
from pyspark.sql import functions as F

db_name = "delta_dlt_demo_db"
base_path = "dbfs:/tmp/delta_dlt_demo"
raw_path  = f"{base_path}/raw"
bronze_tbl = f"{db_name}.bronze_sales"
silver_tbl = f"{db_name}.silver_sales"
gold_tbl   = f"{db_name}.gold_sales_agg"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{base_path}/db'")

print(f"Using database: {db_name}")
print(f"Base path     : {base_path}")
print(f"Raw path      : {raw_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the sample CSV
# MAGIC If your workspace allows outbound internet, we download the CSV from GitHub.
# MAGIC Otherwise, manually upload `data.csv` to `/FileStore/tables/delta_dlt_demo/raw/` and skip this cell.

# COMMAND ----------

# MAGIC %python
import os, requests, io
from pyspark.sql import SparkSession

dbutils.fs.mkdirs(raw_path)

url = "https://raw.githubusercontent.com/NubeEra-Samples/DataSets/master/data.csv"
try:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data_bytes = r.content
    tmp_local = "/tmp/data.csv"
    with open(tmp_local, "wb") as f:
        f.write(data_bytes)
    dbutils.fs.cp(f"file:{tmp_local}", f"{raw_path}/data.csv", recurse=False)
    print("Downloaded and saved to", f"{raw_path}/data.csv")
except Exception as e:
    print("Download failed or blocked. If so, please upload manually to:", raw_path, "\nError:", e)

display(dbutils.fs.ls(raw_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Delta **bronze** table
# MAGIC Read CSV → write **Delta**. This enforces a schema and provides ACID transactions.

# COMMAND ----------

# MAGIC %python
schema = "InvoiceNo STRING, StockCode STRING, Description STRING, Quantity INT, InvoiceDate STRING, UnitPrice DOUBLE, CustomerID STRING, Country STRING"
df = (spark.read
      .option("header", True)
      .schema(schema)
      .csv(raw_path))

(df
 .withColumn("InvoiceTimestamp", F.to_timestamp("InvoiceDate"))
 .write
 .format("delta")
 .mode("overwrite")
 .save(f"{base_path}/bronze_sales"))

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {bronze_tbl}
  USING DELTA
  LOCATION '{base_path}/bronze_sales'
""")

spark.sql(f"OPTIMIZE {bronze_tbl}")
spark.sql(f"VACUUM {bronze_tbl} RETAIN 168 HOURS")  # 7 days history

display(spark.table(bronze_tbl).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ACID Transactions demo
# MAGIC Let's `UPDATE`, `DELETE`, and `MERGE` into Delta and observe transactionality.

# COMMAND ----------

# MAGIC %sql
-- Simple UPDATE
UPDATE ${db_name}.bronze_sales
SET Quantity = Quantity + 1
WHERE Quantity BETWEEN 1 AND 2;

-- DELETE a small subset
DELETE FROM ${db_name}.bronze_sales
WHERE UnitPrice < 0.1;

-- Show count afterwards
SELECT COUNT(*) AS rows_after_mutations FROM ${db_name}.bronze_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time Travel
# MAGIC Query **previous versions** using `VERSION AS OF`.

# COMMAND ----------

# MAGIC %python
history = spark.sql(f"DESCRIBE HISTORY {bronze_tbl}")
display(history)

last_version = history.agg(F.max("version")).first()[0]
prev_version = max(0, (last_version or 0) - 1)
print("Current version:", last_version, "Previous:", prev_version)

display(spark.sql(f"SELECT * FROM {bronze_tbl} VERSION AS OF {prev_version} LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Enforcement & Evolution
# MAGIC Attempt to write a new column (e.g., `Channel`) with **mergeSchema** for evolution.

# COMMAND ----------

# MAGIC %python
new_df = (spark.table(bronze_tbl)
          .withColumn("Channel", F.lit("online")))

(new_df.write
 .format("delta")
 .mode("overwrite")
 .option("mergeSchema", "true")
 .save(f"{base_path}/bronze_sales"))

display(spark.table(bronze_tbl).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create **silver** and **gold** tables
# MAGIC - **Silver**: clean types, filter bad rows
# MAGIC - **Gold**: aggregated sales by country

# COMMAND ----------

# MAGIC %python
bronze = spark.table(bronze_tbl)

silver = (bronze
          .filter(F.col("Quantity").isNotNull() & (F.col("Quantity") > 0))
          .withColumn("Amount", F.col("Quantity") * F.col("UnitPrice"))
          .withColumn("InvoiceDate", F.to_date("InvoiceTimestamp")))

(silver.write
 .format("delta")
 .mode("overwrite")
 .save(f"{base_path}/silver_sales"))

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_tbl}
USING DELTA
LOCATION '{base_path}/silver_sales'
""")

gold = (silver.groupBy("Country")
        .agg(F.sum("Amount").alias("TotalAmount"),
             F.countDistinct("InvoiceNo").alias("DistinctInvoices")))

(gold.write
 .format("delta")
 .mode("overwrite")
 .save(f"{base_path}/gold_sales_agg"))

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {gold_tbl}
USING DELTA
LOCATION '{base_path}/gold_sales_agg'
""")

display(spark.table(gold_tbl).orderBy(F.desc("TotalAmount")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimizations
# MAGIC - **CACHE** frequently used table
# MAGIC - **OPTIMIZE** with **ZORDER** to cluster by `Country`
# MAGIC - (Optional) **REORG** small files (**auto-optimized** in newer runtimes)

# COMMAND ----------

# MAGIC %sql
CACHE TABLE ${db_name}.silver_sales;
SELECT COUNT(*) FROM ${db_name}.silver_sales; -- warms the cache

OPTIMIZE ${db_name}.silver_sales ZORDER BY (Country);
OPTIMIZE ${db_name}.gold_sales_agg ZORDER BY (Country);

VACUUM ${db_name}.silver_sales RETAIN 168 HOURS;
VACUUM ${db_name}.gold_sales_agg RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming concept (readStream → append to bronze)
# MAGIC For a simple demo, we'll stream from the `raw` folder (append new files to it).

# COMMAND ----------

# MAGIC %python
stream_src = f"{raw_path}/stream"
dbutils.fs.mkdirs(stream_src)

schema_stream = "InvoiceNo STRING, StockCode STRING, Description STRING, Quantity INT, InvoiceDate STRING, UnitPrice DOUBLE, CustomerID STRING, Country STRING"

stream_df = (spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("header", True)
             .schema(schema_stream)
             .load(stream_src))

query = (stream_df
         .withColumn("InvoiceTimestamp", F.to_timestamp("InvoiceDate"))
         .writeStream
         .format("delta")
         .option("checkpointLocation", f"{base_path}/_checkpoints/bronze_stream")
         .outputMode("append")
         .start(f"{base_path}/bronze_streamed"))

print("Streaming started. To simulate data arrival, copy another CSV into:", stream_src)

# COMMAND ----------

# MAGIC %python
# After a few seconds, stop the stream (or leave running for demo)
import time; time.sleep(5)
for q in spark.streams.active:
    if q.name == query.name:
        q.stop()
print("Stream stopped.")
