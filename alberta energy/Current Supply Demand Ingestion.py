# Databricks notebook source
# MAGIC %md ### Import Helper AESO API Function

# COMMAND ----------

from aeso_api import aeso
aeso_api_key = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJvaXNrM2EiLCJpYXQiOjE2Nzc2OTMyOTh9.wojWs2r9ecH88GbPpy34FShwhkjXPJXBAijY8S3rNAw'
aeso_instance = aeso(aeso_api_key)

# COMMAND ----------

# MAGIC %md ### Get AESO Current Supply Demand
# MAGIC Retrieve data from API

# COMMAND ----------

from pyspark.sql import Row

resp = aeso_instance.get_current_supply_demand()

# Convert the main object (excluding nested lists) into a Row
main_data = Row(
    **{
        k: v
        for k, v in resp.__dict__.items()
        if k not in ["generation_data_list", "interchange_list"]
    }
)

# Create DataFrame for the main object
df_main = spark.createDataFrame([main_data])

# For the nested lists, create separate DataFrames
# Generation Data List
generation_rows = [Row(**gen_data.__dict__) for gen_data in resp.generation_data_list]
df_generation = spark.createDataFrame(generation_rows)

# Interchange List
interchange_rows = [Row(**inter_data.__dict__) for inter_data in resp.interchange_list]
df_interchange = spark.createDataFrame(interchange_rows)

# Explode the generation_data_list and interchange_list and join with the main DataFrame
df_main = df_main.crossJoin(df_generation).crossJoin(df_interchange)

df_main.write.mode("overwrite").saveAsTable("gshen_catalog.aeso.csd_snapshot")

# COMMAND ----------

# MAGIC %md ### Write to Ingestion Table & Append Data using SCD Type I

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gshen_catalog.aeso.csd_raw
# MAGIC USING DELTA
# MAGIC AS SELECT *
# MAGIC FROM gshen_catalog.aeso.csd_snapshot
# MAGIC WHERE 1 = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gshen_catalog.aeso.csd_raw as target USING gshen_catalog.aeso.csd_snapshot as source ON target.last_updated_datetime_utc = source.last_updated_datetime_utc
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   *
