# Databricks notebook source
# MAGIC %md ### Import Helper AESO API Function

# COMMAND ----------

from aeso_api import aeso

aeso_api_key = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJvaXNrM2EiLCJpYXQiOjE2Nzc2OTMyOTh9.wojWs2r9ecH88GbPpy34FShwhkjXPJXBAijY8S3rNAw'
aeso_instance = aeso(aeso_api_key)

# COMMAND ----------

# MAGIC %md ### Get AESO Pool Price Data
# MAGIC Retrieve data from API

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime, timedelta
import pytz
import pandas as pd


# Get Current Date, and Current Date + 1
utc_time = pytz.utc.localize(datetime.utcnow())
calgary_time = utc_time.astimezone(pytz.timezone("America/Edmonton"))

current_date = calgary_time.strftime("%Y-%m-%d")
tomorrow_date = (calgary_time + timedelta(days=1)).strftime("%Y-%m-%d")
print(f"Current Date: {current_date}")
print(f"Tomorrow Date: {tomorrow_date}")
resp = aeso_instance.get_pool_price_report(
    start_date=current_date, end_date=tomorrow_date
)
pdf = pd.DataFrame(resp)
df = spark.createDataFrame(pdf)

display(df)

df.write.mode("overwrite").saveAsTable("gshen_catalog.aeso.pool_price_snapshot")

# COMMAND ----------

# MAGIC %md ### Write to Ingestion Table & Append Data using SCD Type I

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gshen_catalog.aeso.pool_price_raw
# MAGIC USING DELTA
# MAGIC AS SELECT *
# MAGIC FROM gshen_catalog.aeso.pool_price_snapshot
# MAGIC WHERE 1 = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gshen_catalog.aeso.pool_price_raw as target USING gshen_catalog.aeso.pool_price_snapshot as source ON target.begin_datetime_mpt = source.begin_datetime_mpt
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE
# MAGIC SET
# MAGIC   target.pool_price = source.pool_price,
# MAGIC   target.rolling_30day_avg = source.rolling_30day_avg
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   *
