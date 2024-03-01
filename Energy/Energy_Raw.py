# Databricks notebook source
# Define file paths
Demo = "/mnt/blobstorage/demo.csv"

# COMMAND ----------

# Read Data from the headers file
DF = spark.read.format("csv").load(Demo)

# COMMAND ----------

# Define catalog name and schema parameters
catalog_name = "onpointpoc"
schema = "Energy_bronze"

# COMMAND ----------

DF.write.format("delta").mode("append").saveAsTable(f"{catalog_name}.{schema}.Energy_Bronze")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# Specify the catalog name and schema name
catalog_name = "onpointpoc"
schema_name = "energy_bronze"
table_name = "energy_bronze"

# Read the table from the specified schema in the catalog
Energy_DF = spark.read.table(f"{catalog_name}.{schema_name}.{table_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM onpointpoc.energy_bronze.energy_bronze t2
# MAGIC   WHERE t2._c0 IS NULL AND t2.`_c1` IS NULL AND t2._c2 IS NULL AND t2.`_c3` IS NULL AND t2._c4 IS NULL AND t2.`_c5` IS NULL AND t2.`_c6` IS NULL AND t2._c7 IS NULL AND t2.`_c8` IS NULL AND t2.`_c9` IS NULL AND t2._c10 IS NULL AND t2.`_c11` IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH RowNumbers AS (
# MAGIC SELECT *,
# MAGIC        ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS RowNumber
# MAGIC FROM onpointpoc.energy_bronze.energy_bronze
# MAGIC )
# MAGIC SELECT
# MAGIC CONCAT(
# MAGIC   IFNULL("",MAX(CASE WHEN RowNumber = 1 THEN _c0 ELSE NULL END))," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 2 THEN _c0 ELSE NULL END)," "
# MAGIC  ,MAX(CASE WHEN RowNumber = 3 THEN _c0 ELSE NULL END)," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 4 THEN _c0 ELSE NULL END)
# MAGIC ) AS _c0,
# MAGIC CONCAT(
# MAGIC   IFNULL("",MAX(CASE WHEN RowNumber = 1 THEN _c1 ELSE NULL END))," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 2 THEN _c1 ELSE NULL END)," "
# MAGIC  ,MAX(CASE WHEN RowNumber = 3 THEN _c1 ELSE NULL END)," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 4 THEN _c1 ELSE NULL END)
# MAGIC ) AS _c1,
# MAGIC CONCAT(
# MAGIC   IFNULL("",MAX(CASE WHEN RowNumber = 1 THEN _c2 ELSE NULL END))," "
# MAGIC   ,IFNULL("",MAX(CASE WHEN RowNumber = 2 THEN _c2 ELSE NULL END))," "
# MAGIC  ,MAX(CASE WHEN RowNumber = 3 THEN _c2 ELSE NULL END)," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 4 THEN _c2 ELSE NULL END)
# MAGIC ) AS _c2,
# MAGIC CONCAT(
# MAGIC   IFNULL("",MAX(CASE WHEN RowNumber = 1 THEN _c3 ELSE NULL END))," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 2 THEN _c3 ELSE NULL END)," "
# MAGIC  ,MAX(CASE WHEN RowNumber = 3 THEN _c3 ELSE NULL END)," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 4 THEN _c3 ELSE NULL END)
# MAGIC ) AS _c3,
# MAGIC CONCAT(
# MAGIC   IFNULL("",MAX(CASE WHEN RowNumber = 1 THEN _c4 ELSE NULL END))," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 2 THEN _c4 ELSE NULL END)," "
# MAGIC  ,MAX(CASE WHEN RowNumber = 3 THEN _c4 ELSE NULL END)," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 4 THEN _c4 ELSE NULL END)
# MAGIC ) AS _c4,
# MAGIC CONCAT(
# MAGIC   IFNULL("",MAX(CASE WHEN RowNumber = 1 THEN _c5 ELSE NULL END))," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 2 THEN _c5 ELSE NULL END)," "
# MAGIC  ,MAX(CASE WHEN RowNumber = 3 THEN _c5 ELSE NULL END)," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 4 THEN _c5 ELSE NULL END)
# MAGIC ) AS _c5,
# MAGIC CONCAT(
# MAGIC   IFNULL("",MAX(CASE WHEN RowNumber = 1 THEN _c6 ELSE NULL END))," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 2 THEN _c6 ELSE NULL END)," "
# MAGIC  ,MAX(CASE WHEN RowNumber = 3 THEN _c6 ELSE NULL END)," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 4 THEN _c6 ELSE NULL END)
# MAGIC ) AS _c6,
# MAGIC CONCAT(
# MAGIC   IFNULL("",MAX(CASE WHEN RowNumber = 1 THEN _c7 ELSE NULL END))," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 2 THEN _c7 ELSE NULL END)," "
# MAGIC  ,MAX(CASE WHEN RowNumber = 3 THEN _c7 ELSE NULL END)," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 4 THEN _c7 ELSE NULL END)
# MAGIC ) AS _c7,
# MAGIC CONCAT(
# MAGIC   IFNULL("",MAX(CASE WHEN RowNumber = 1 THEN _c8 ELSE NULL END))," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 2 THEN _c8 ELSE NULL END)," "
# MAGIC  ,MAX(CASE WHEN RowNumber = 3 THEN _c8 ELSE NULL END)," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 4 THEN _c8 ELSE NULL END)
# MAGIC ) AS _c8,
# MAGIC CONCAT(
# MAGIC   MAX(CASE WHEN RowNumber = 1 THEN _c9 ELSE NULL END)," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 2 THEN _c9 ELSE NULL END)," "
# MAGIC  ,MAX(CASE WHEN RowNumber = 3 THEN _c9 ELSE NULL END)," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 4 THEN _c9 ELSE NULL END)
# MAGIC ) AS _c9,
# MAGIC CONCAT(
# MAGIC   IFNULL("",MAX(CASE WHEN RowNumber = 1 THEN _c10 ELSE NULL END))," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 2 THEN _c10 ELSE NULL END)," "
# MAGIC  ,MAX(CASE WHEN RowNumber = 3 THEN _c10 ELSE NULL END)," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 4 THEN _c10 ELSE NULL END)
# MAGIC ) AS _c10,
# MAGIC CONCAT(
# MAGIC   MAX(CASE WHEN RowNumber = 1 THEN _c11 ELSE NULL END)," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 2 THEN _c11 ELSE NULL END)," "
# MAGIC  ,MAX(CASE WHEN RowNumber = 3 THEN _c11 ELSE NULL END)," "
# MAGIC   ,MAX(CASE WHEN RowNumber = 4 THEN _c11 ELSE NULL END)
# MAGIC ) AS _c11
# MAGIC FROM RowNumbers
# MAGIC

# COMMAND ----------

# Define catalog name and schema parameters
catalog_name = "onpointpoc"
schema = "energy_bronze"

# COMMAND ----------

_sqldf.write.format("delta").mode("append").saveAsTable(f"{catalog_name}.{schema}.Energy_Headers")

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH A AS (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber
# MAGIC     FROM onpointpoc.energy_bronze.energy_bronze
# MAGIC )
# MAGIC SELECT * FROM A WHERE RowNumber > 4;

# COMMAND ----------

# Drop the column
col_name = "RowNumber"
df = _sqldf.drop(col_name)

# COMMAND ----------

# Define catalog name and schema parameters
catalog_name = "onpointpoc"
schema = "energy_bronze"

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable(f"{catalog_name}.{schema}.Energy_Data")
