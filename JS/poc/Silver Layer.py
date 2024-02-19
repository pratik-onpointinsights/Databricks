# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# Specify the catalog name and schema name
catalog_name = "onpointpoc"
schema_name = "js_bronze"
table_name = "jssales_bronze"

# Read the table from the specified schema in the catalog
JSSales_DF = spark.read.table(f"{catalog_name}.{schema_name}.{table_name}")


# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

jssales_selected_df = JSSales_DF.select(col("ship_date"),col("bill_to_id"), col("ship_to_id"), col("sales_order_id"), col("line_id"), col("quantity"), col("price"), col("cogs"), col("SalesAmt"), col("COGSAmt"), col("ProductSegment"), col("ProductSubSegment"), col("ProductDescription"), col("PriceLineDescription"), col("BuyLineDescription"), col("ProductInitiative"), col("sales_source"))

# COMMAND ----------

# from pyspark.sql.functions import current_timestamp
# def add_ingestion_date(input_df):
#  output_df = input_df.withColumn("ingestion_date", current_timestamp())
#  return output_df

# COMMAND ----------

# JSSales_final_df = add_ingestion_date(jssales_selected_df)

# COMMAND ----------

jssales_renamed_df = jssales_selected_df.withColumnRenamed("SalesAmt", "sales_amount") \
.withColumnRenamed("COGSAmt", "cogs_amount") \
.withColumnRenamed("ProductSegment", "product_segment") \
.withColumnRenamed("ProductSubSegment", "product_sub_segment") \
.withColumnRenamed("ProductDescription", "product_description") \
.withColumnRenamed("PriceLineDescription", "price_line_description") \
.withColumnRenamed("ProductInitiative", "product_category") \
.withColumnRenamed("salessource", "sales_soruce")\
.withColumn("inserted_on", current_timestamp())


# COMMAND ----------

# Define catalog name and schema parameters
catalog_name = "onpointpoc"
schema = "JS_Silver"

# COMMAND ----------

jssales_renamed_df.write.format("delta").mode("append").saveAsTable(f"{catalog_name}.{schema}.JSSales_Silver")
