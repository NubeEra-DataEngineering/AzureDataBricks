-- Databricks notebook source
--- Show all catalogs in the metastore
SHOW CATALOGS;

-- COMMAND ----------

--- create a new catalog (optional)
CREATE CATALOG IF NOT EXISTS lineage_data;

-- COMMAND ----------

-- Set the current catalog
USE CATALOG lineage_data;

-- COMMAND ----------

--- Check grants on the lineage_data catalog
SHOW GRANTS ON CATALOG lineage_data;

-- COMMAND ----------

--- Make sure that all required users have USE CATALOG priviledges on the catalog. 
--- In this example, we grant the priviledge to all account users.
GRANT USE CATALOG
ON CATALOG lineage_data
TO `account users`;

-- COMMAND ----------

-- Show schemas in the selected catalog
SHOW SCHEMAS;

-- COMMAND ----------

-- Create Schema
CREATE SCHEMA lineage_data.lineagedemo;

-- COMMAND ----------

--- Make sure that all required users have USE CATALOG priviledges on the catalog. 
--- In this example, we grant the priviledge to all account users.
GRANT USAGE, CREATE ON SCHEMA lineage_data.lineagedemo TO `account users`;

-- COMMAND ----------

-- Create Table
CREATE TABLE IF NOT EXISTS
  lineage_data.lineagedemo.menu (
    recipe_id INT,
    app string,
    main string,
    dessert string
  );

-- COMMAND ----------

-- Insert Data

INSERT INTO lineage_data.lineagedemo.menu
    (recipe_id, app, main, dessert)
VALUES
    (1,"Ceviche", "Tacos", "Flan"),
    (2,"Tomato Soup", "Souffle", "Creme Brulee"),
    (3,"Chips","Grilled Cheese","Cheesecake");

-- COMMAND ----------

-- Create Table using Select
CREATE TABLE
  lineage_data.lineagedemo.dinner
AS SELECT
  recipe_id, concat(app," + ", main," + ",dessert)
AS
  full_menu
FROM
  lineage_data.lineagedemo.menu


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Please Check into Lineage Graph Explorer
-- MAGIC ## 1. Catalog --> Metastore --> Catalog --> Schema
-- MAGIC ## 2.  Table(dinner) --> See Lineage Graph

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import rand, round
-- MAGIC df = spark.range(3).withColumn("price", round(10*rand(seed=42),2)).withColumnRenamed("id","recipe_id")
-- MAGIC
-- MAGIC df.write.mode("overwrite").saveAsTable("lineage_data.lineagedemo.price")
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dinner = spark.read.table("lineage_data.lineagedemo.dinner")
-- MAGIC price = spark.read.table("lineage_data.lineagedemo.price")
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dinner_price = dinner.join(price, on="recipe_id")
-- MAGIC dinner_price.write.mode("overwrite").saveAsTable("lineage_data.lineagedemo.dinner_price")

-- COMMAND ----------

SELECT * FROM lineage_data.lineagedemo.menu

-- COMMAND ----------

GRANT USE SCHEMA on lineage_data.lineagedemo to `sam@company.com`;

-- COMMAND ----------

GRANT SELECT on lineage_data.lineagedemo.menu
 to `francis@company.com`;

-- COMMAND ----------

GRANT BROWSE on lineage_data to `mujahed@company.com`;
