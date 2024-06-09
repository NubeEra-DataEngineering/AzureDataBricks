-- Databricks notebook source
-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Volumes Unity Catalog Access Control
-- MAGIC This notebook provides an example workflow for creating your first Volume in Unity Catalog:
-- MAGIC
-- MAGIC - Choose a catalog and a schema, or create new ones.
-- MAGIC - Create a managed Volume under the chosen schema.
-- MAGIC - Browse the Volume using the three-level namespace.
-- MAGIC - Manage the Volume's access permissions.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Requirements
-- MAGIC ## Your workspace must be attached to a Unity Catalog metastore. See https://docs.databricks.com/data-governance/unity-catalog/get-started.html.
-- MAGIC ## Your notebook is attached to a cluster that uses DBR 13.2+ and uses the single user or shared cluster access mode.

-- COMMAND ----------

-- catalog --> Schema --> Volume

-- COMMAND ----------

--- Show all catalogs in the metastore
SHOW CATALOGS;

-- COMMAND ----------

--- create a new catalog (optional)
CREATE CATALOG IF NOT EXISTS quickstart_catalog;

-- COMMAND ----------

-- Set the current catalog
USE CATALOG quickstart_catalog;

-- COMMAND ----------

--- Check grants on the quickstart catalog
SHOW GRANTS ON CATALOG quickstart_catalog;

-- COMMAND ----------


--- Make sure that all required users have USE CATALOG priviledges on the catalog. 
--- In this example, we grant the priviledge to all account users.
GRANT USE CATALOG
ON CATALOG quickstart_catalog
TO `account users`;

-- COMMAND ----------

-- Show schemas in the selected catalog
SHOW SCHEMAS;

-- COMMAND ----------

--- Create a new schema in the quick_start catalog
CREATE SCHEMA IF NOT EXISTS quickstart_schema
COMMENT "A new Unity Catalog schema called quickstart_schema";

-- COMMAND ----------

-- Describe a schema
DESCRIBE SCHEMA EXTENDED quickstart_schema;

-- COMMAND ----------

USE SCHEMA quickstart_schema;


-- COMMAND ----------

--- Grant CREATE VOLUME on a Catalog or Schema.
--- When granted at Catalog level, users will be able to create Volumes on any schema in this Catalog.
GRANT CREATE VOLUME
ON CATALOG quickstart_catalog
TO `account users`;

-- COMMAND ----------

--- Create an external volume under the newly created directory
CREATE VOLUME IF NOT EXISTS quickstart_catalog.quickstart_schema.quickstart_volume
COMMENT 'This is my example managed volume'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC strURLSource="dbfs:/databricks-datasets/wine-quality/winequality-red.csv"
-- MAGIC strURLTarget="/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/"
-- MAGIC
-- MAGIC dbutils.fs.cp(strURLSource,strURLTarget)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/")

-- COMMAND ----------

-- View all Volumes in a schema
SHOW VOLUMES IN quickstart_schema;

-- COMMAND ----------

ALTER VOLUME quickstart_catalog.quickstart_schema.quickstart_volume SET OWNER TO `account users`

-- COMMAND ----------

COMMENT ON VOLUME quickstart_catalog.quickstart_schema.quickstart_volume IS 'This is a shared Volume';

-- COMMAND ----------

--- Lists all privileges that are granted on a Volume.
SHOW GRANTS ON VOLUME quickstart_catalog.quickstart_schema.quickstart_volume;

-- COMMAND ----------

-- Grant READ VOLUME on a volume
GRANT READ VOLUME
ON VOLUME quickstart_catalog.quickstart_schema.quickstart_volume
TO `account users`;

-- COMMAND ----------

-- Grant WRITE VOLUME on a volume
GRANT WRITE VOLUME
ON VOLUME quickstart_catalog.quickstart_schema.quickstart_volume
TO `account users`;

-- COMMAND ----------

--- Lists all privileges that are granted on a Volume.
SHOW GRANTS ON VOLUME quickstart_catalog.quickstart_schema.quickstart_volume;

-- COMMAND ----------

--- Revokes a previously granted privilege on a Volume.
REVOKE WRITE VOLUME
ON VOLUME quickstart_catalog.quickstart_schema.quickstart_volume
FROM `account users`;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs("/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/destination")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mv("/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/winequality-red.csv", "/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/destination/")

-- COMMAND ----------

SELECT * FROM csv.`dbfs:/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/destination/winequality-red.csv`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1 = spark.read.format("csv").load("dbfs:/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/destination/winequality-red.csv")
-- MAGIC display(df1)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val df1 = spark.read.format("csv").load("dbfs:/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/destination/winequality-red.csv")

-- COMMAND ----------

-- MAGIC %sh ls /Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/

-- COMMAND ----------

-- MAGIC %sh echo "hi" >> /Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/file.txt

-- COMMAND ----------

-- MAGIC %sh cat /Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/file.txt

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os
-- MAGIC os.listdir('/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/')

-- COMMAND ----------

-- MAGIC
-- MAGIC %python
-- MAGIC f = open('/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/destination/winequality-red.csv', 'r')
-- MAGIC print(f.read())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC  
-- MAGIC import pandas as pd
-- MAGIC  
-- MAGIC df1 = pd.read_csv("/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/destination/winequality-red.csv")

-- COMMAND ----------

-- MAGIC %r
-- MAGIC  
-- MAGIC require(SparkR)
-- MAGIC  
-- MAGIC df1 <- read.df("/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/destination/winequality-red.csv", "csv")
