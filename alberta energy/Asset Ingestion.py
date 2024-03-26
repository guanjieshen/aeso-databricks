# Databricks notebook source
# MAGIC %md ### Import Helper AESO API Function

# COMMAND ----------

from aeso_api import aeso
aeso_api_key = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJvaXNrM2EiLCJpYXQiOjE2Nzc2OTMyOTh9.wojWs2r9ecH88GbPpy34FShwhkjXPJXBAijY8S3rNAw'
aeso_instance = aeso(aeso_api_key)

# COMMAND ----------

# MAGIC %md ### Get AESO Asset List
# MAGIC Retrieve data from API

# COMMAND ----------

from pyspark.sql import Row
import pandas as pd

resp= aeso_instance.get_asset_list()
pdf = pd.DataFrame(resp)
asset_df=spark.createDataFrame(pdf) 
display(asset_df)

asset_df.write.mode("overwrite").saveAsTable("gshen_catalog.aeso.assets_raw")
