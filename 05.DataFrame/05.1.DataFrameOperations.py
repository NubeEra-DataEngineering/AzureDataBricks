# Databricks notebook source
# import pyspark class Row from module sql
from pyspark.sql import *

# Create Example Data - Departments and Employees

# Create the Departments
department1 = Row(id='123456', name='Computer Science')
department2 = Row(id='789012', name='Mechanical Engineering')
department3 = Row(id='345678', name='Theater and Drama')
department4 = Row(id='901234', name='Indoor Recreation')

# Create the Employees
Employee = Row("firstName", "lastName", "email", "salary")
employee1 = Employee('michael', 'armbrust', 'no-reply@berkeley.edu', 100000)
employee2 = Employee('xiangrui', 'meng', 'no-reply@stanford.edu', 120000)
employee3 = Employee('matei', None, 'no-reply@waterloo.edu', 140000)
employee4 = Employee(None, 'wendell', 'no-reply@berkeley.edu', 160000)
employee5 = Employee('michael', 'jackson', 'no-reply@neverla.nd', 80000)

# Create the DepartmentWithEmployees instances from Departments and Employees
departmentWithEmployees1 = Row(department=department1, employees=[employee1, employee2])
departmentWithEmployees2 = Row(department=department2, employees=[employee3, employee4])
departmentWithEmployees3 = Row(department=department3, employees=[employee5, employee4])
departmentWithEmployees4 = Row(department=department4, employees=[employee2, employee3])

print(department1)
print(employee2)
print(departmentWithEmployees1.employees[0].email)

# COMMAND ----------

departmentsWithEmployeesSeq1 = [departmentWithEmployees1, departmentWithEmployees2]
df1 = spark.createDataFrame(departmentsWithEmployeesSeq1)

display(df1)

departmentsWithEmployeesSeq2 = [departmentWithEmployees3, departmentWithEmployees4]
df2 = spark.createDataFrame(departmentsWithEmployeesSeq2)

display(df2)

# COMMAND ----------

unionDF = df1.union(df2)
display(unionDF)

# COMMAND ----------

# Remove the file if it exists
dbutils.fs.rm("/tmp/databricks-df-example.parquet", True)
unionDF.write.parquet("/tmp/databricks-df-example.parquet")

# COMMAND ----------

parquetDF = spark.read.parquet("/tmp/databricks-df-example.parquet")
display(parquetDF)

# COMMAND ----------

from pyspark.sql.functions import explode

explodeDF = unionDF.select(explode("employees").alias("e"))
flattenDF = explodeDF.selectExpr("e.firstName", "e.lastName", "e.email", "e.salary")

flattenDF.show()

# COMMAND ----------

filterDF = flattenDF.filter(flattenDF.firstName == "xiangrui").sort(flattenDF.lastName)
display(filterDF)

# COMMAND ----------

from pyspark.sql.functions import col, asc

# Use `|` instead of `or`
filterDF = flattenDF.filter((col("firstName") == "xiangrui") | (col("firstName") == "michael")).sort(asc("lastName"))
display(filterDF)

# COMMAND ----------

nonNullDF = flattenDF.fillna("--")
display(nonNullDF)

# COMMAND ----------

from pyspark.sql.functions import countDistinct

countDistinctDF = nonNullDF.select("firstName", "lastName")\
  .groupBy("firstName")\
  .agg(countDistinct("lastName").alias("distinct_last_names"))

display(countDistinctDF)

# COMMAND ----------

countDistinctDF.explain()

# COMMAND ----------

# register the DataFrame as a temp view so that we can query it using SQL
nonNullDF.createOrReplaceTempView("databricks_df_example")

# Perform the same query as the DataFrame above and return ``explain``
countDistinctDF_sql = spark.sql('''
  SELECT firstName, count(distinct lastName) AS distinct_last_names
  FROM databricks_df_example
  GROUP BY firstName
''')

countDistinctDF_sql.explain()

# COMMAND ----------

salarySumDF = nonNullDF.agg({"salary" : "sum"})
display(salarySumDF)

# COMMAND ----------

type(nonNullDF.salary)

# COMMAND ----------

nonNullDF.describe("salary").show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
plt.clf()
pdDF = nonNullDF.toPandas()
pdDF.plot(x='firstName', y='salary', kind='bar', rot=45)
display()

# COMMAND ----------

dbutils.fs.rm("/tmp/databricks-df-example.parquet", True)

# COMMAND ----------

#How can I get better performance with DataFrame UDFs?
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Build an example DataFrame dataset to work with.
dbutils.fs.rm("/tmp/dataframe_sample.csv", True)
dbutils.fs.put("/tmp/dataframe_sample.csv", """id|end_date|start_date|location
1|2015-10-14 00:00:00|2015-09-14 00:00:00|CA-SF
2|2015-10-15 01:00:20|2015-08-14 00:00:00|CA-SD
3|2015-10-16 02:30:00|2015-01-14 00:00:00|NY-NY
4|2015-10-17 03:00:20|2015-02-14 00:00:00|NY-NY
5|2015-10-18 04:30:00|2014-04-14 00:00:00|CA-SD
""", True)

df = spark.read.format("csv").options(header='true', delimiter = '|').load("/tmp/dataframe_sample.csv")
df.printSchema()


# COMMAND ----------

# Instead of registering a UDF, call the builtin functions to perform operations on the columns.
# This will provide a performance improvement as the builtins compile and run in the platform's JVM.

# Convert to a Date type
df = df.withColumn('date', F.to_date(df.end_date))

# Parse out the date only
df = df.withColumn('date_only', F.regexp_replace(df.end_date,' (\d+)[:](\d+)[:](\d+).*$', ''))

# Split a string and index a field
df = df.withColumn('city', F.split(df.location, '-')[1])

# Perform a date diff function
df = df.withColumn('date_diff', F.datediff(F.to_date(df.end_date), F.to_date(df.start_date)))

# COMMAND ----------

df.createOrReplaceTempView("sample_df")
display(sql("select * from sample_df"))

# COMMAND ----------

rdd_json = df.toJSON()
rdd_json.take(2)

# COMMAND ----------

from pyspark.sql import functions as F

add_n = udf(lambda x, y: x + y, IntegerType())

# We register a UDF that adds a column to the DataFrame, and we cast the id column to an Integer type.
df = df.withColumn('id_offset', add_n(F.lit(1000), df.id.cast(IntegerType())))

# COMMAND ----------

display(df)

# COMMAND ----------

# any constants used by UDF will automatically pass through to workers
N = 90
last_n_days = udf(lambda x: x < N, BooleanType())

df_filtered = df.filter(last_n_days(df.date_diff))
display(df_filtered)

# COMMAND ----------

# Both return DataFrame types
df_1 = table("sample_df")
df_2 = spark.sql("select * from sample_df")

# COMMAND ----------

sqlContext.clearCache()
sqlContext.cacheTable("sample_df")
sqlContext.uncacheTable("sample_df")

# COMMAND ----------

# Provide the min, count, and avg and groupBy the location column. Diplay the results
agg_df = df.groupBy("location").agg(F.min("id"), F.count("id"), F.avg("date_diff"))
display(agg_df)

# COMMAND ----------

df = df.withColumn('end_month', F.month('end_date'))
df = df.withColumn('end_year', F.year('end_date'))
df.write.partitionBy("end_year", "end_month").parquet("/tmp/sample_table")
display(dbutils.fs.ls("/tmp/sample_table"))

# COMMAND ----------

null_item_schema = StructType([StructField("col1", StringType(), True),
                               StructField("col2", IntegerType(), True)])
null_df = spark.createDataFrame([("test", 1), (None, 2)], null_item_schema)
display(null_df.filter("col1 IS NOT NULL"))

# COMMAND ----------

adult_df = spark.read.format("com.spark.csv").option("header", "false").option("inferSchema", "true").load("dbfs:/databricks-datasets/adult/adult.data")
adult_df.printSchema()

# COMMAND ----------


