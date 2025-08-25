
# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables: Bronze → Silver → Gold with Expectations
# MAGIC This DLT pipeline reads CSV files, enforces quality rules, and builds incremental tables.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

raw_path = "dbfs:/tmp/delta_dlt_demo/raw"

@dlt.table(
    comment="Raw sales data as Bronze Delta table (incremental).",
    table_properties={"quality": "bronze"}
)
def bronze_sales():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("header", True)
             .load(raw_path)
             .withColumn("Quantity", F.col("Quantity").cast("int"))
             .withColumn("UnitPrice", F.col("UnitPrice").cast("double"))
             .withColumn("InvoiceTimestamp", F.to_timestamp("InvoiceDate"))
    )

@dlt.expect_or_drop("valid_quantity", "Quantity IS NOT NULL AND Quantity > 0")
@dlt.expect_or_drop("valid_unitprice", "UnitPrice IS NOT NULL AND UnitPrice >= 0")
@dlt.table(
    comment="Cleaned Silver sales with computed Amount.",
    table_properties={"quality": "silver"}
)
def silver_sales():
    return (
        dlt.read_stream("bronze_sales")
           .withColumn("Amount", F.col("Quantity") * F.col("UnitPrice"))
           .withColumn("InvoiceDate", F.to_date("InvoiceTimestamp"))
    )

@dlt.table(
    comment="Gold aggregate by Country with totals and distinct invoices.",
    table_properties={"quality": "gold"}
)
def gold_sales_by_country():
    return (
        dlt.read_stream("silver_sales")
           .groupBy("Country")
           .agg(F.sum("Amount").alias("TotalAmount"),
                F.countDistinct("InvoiceNo").alias("DistinctInvoices"))
    )
