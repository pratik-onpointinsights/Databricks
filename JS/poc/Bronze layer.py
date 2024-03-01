# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze

# COMMAND ----------

# Define file paths
JSSales = "/mnt/blobstorage/JSSales.csv"
Headers = "/mnt/blobstorage/Headers.csv"

# COMMAND ----------

# Read headers from the headers file
Headers_DF = spark.read.format("csv").option("header", "true").load(Headers)

# COMMAND ----------

JSSales_DF = spark.read.format("csv").option("header", "false").load(JSSales)


# COMMAND ----------

# Display the data DataFrame
display(JSSales_DF)

# COMMAND ----------

# Define catalog name and schema parameters
catalog_name = "onpointpoc"
schema = "JS_bronze"

# COMMAND ----------

JSSales_df.write.format("delta").mode("append").saveAsTable(f"{catalog_name}.{schema}.JSSales_Bronze")
