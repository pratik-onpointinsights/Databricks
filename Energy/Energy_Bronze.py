# Databricks notebook source
# MAGIC %sql
# MAGIC DELETE FROM onpointpoc.energy_bronze.energy_data WHERE `_c0` IS NULL and `_c1` IS NOT NULL
# MAGIC

# COMMAND ----------

# Specify the catalog name and schema name
catalog_name = "onpointpoc"
schema_name = "energy_bronze"
table_name = "energy_headers"

# Read the table from the specified schema in the catalog
Energy_Headers = spark.read.table(f"{catalog_name}.{schema_name}.{table_name}")

# COMMAND ----------

# Specify the catalog name and schema name
catalog_name = "onpointpoc"
schema_name = "energy_bronze"
table_name = "energy_data"

# Read the table from the specified schema in the catalog
Energy_Data = spark.read.table(f"{catalog_name}.{schema_name}.{table_name}")

# COMMAND ----------

#Get the number of columns in the data table
num_columns = Energy_Data.schema.fields.__len__()

# COMMAND ----------

headers = Energy_Headers.first()[0:num_columns]

# COMMAND ----------

DF = Energy_Data.toDF(*headers)

# COMMAND ----------

from pyspark.sql.functions import col

# Assuming DF is your DataFrame
DF = DF.withColumn("   NAICS Code(a)", col("   NAICS Code(a)").cast("integer"))

# COMMAND ----------

from pyspark.sql.functions import expr
from pyspark.sql.functions import when


# COMMAND ----------

DF = DF.withColumn("Region", when(DF["     Subsector and Industry"].isNull(), DF["  Total(b) (trillion Btu)"]).otherwise(None))

# COMMAND ----------

DF = DF.withColumn("Region", when(DF["     Subsector and Industry"].isNull(), DF["  Total(b) (trillion Btu)"]).otherwise(None))


# COMMAND ----------

DF = DF.toPandas()

# COMMAND ----------

cols = ['Region']
DF.loc[:, cols] = DF.loc[:, cols].ffill()

# COMMAND ----------

DF = spark.createDataFrame(DF)

# COMMAND ----------

DF = DF.where(col('   NAICS Code(a)').isNotNull())

# COMMAND ----------

# Define the mapping of old column names to new column names
column_mapping = {
    '   NAICS Code(a)': 'NAICS Code',
    '     Subsector and Industry': 'Subsector and Industry',
    '  Total(b) (trillion Btu)': 'Total (trillion Btu)',
    ' Net Electricity(c) (million kWh)': 'Net Electricity (million kWh)',
    ' Residual Fuel Oil (million bbl)': 'Residual Fuel Oil (million bbl)',
    ' Distillate Fuel Oil(d) (million bbl)': 'Distillate Fuel Oil (million bbl)',
    ' Natural  Gas(e) (billion cu ft)': 'Natural  Gas (billion cu ft)',
    ' HGL (excluding natural gasoline)(f) (million bbl)': 'HGL (excluding natural gasoline) (million bbl)',
    ' Coal (million short tons)': 'Coal (million short tons)',
    '   Other(g) (trillion Btu)': 'Other (trillion Btu)'

}

# Rename multiple columns using a loop
for old_name, new_name in column_mapping.items():
    DF = DF.withColumnRenamed(old_name, new_name)


# COMMAND ----------

display(DF)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (
# MAGIC   SELECT
# MAGIC     CASE WHEN LEN(NAICS_Code) = 3 THEN LEFT(NAICS_Code, 3) ELSE NULL END AS NAICS_Main_Sector_Code,
# MAGIC     NAICS_Code AS NAICS_Subsector_Code,
# MAGIC     ABC_Subsector_and_Industry AS Subsector_Value
# MAGIC   FROM DF
# MAGIC )
# MAGIC SELECT
# MAGIC   NAICS_Main_Sector_Code,
# MAGIC   NAICS_Subsector_Code,
# MAGIC   Subsector_Value
# MAGIC FROM cte;
