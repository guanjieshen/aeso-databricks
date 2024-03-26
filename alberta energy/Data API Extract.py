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

# Get Pool Price Data
resp = aeso_instance.get_pool_price_report(
    start_date=current_date, end_date=tomorrow_date
)
pdf = pd.DataFrame(resp)

json_string = pdf.to_json(orient="records")

# COMMAND ----------

# MAGIC %md ##### Generate File Name with Timestamp

# COMMAND ----------

import datetime

# Get the current timestamp as a datetime object
now = datetime.datetime.now()

# Format the datetime object as a string
timestamp_str = now.strftime("%Y_%m_%d %H_%M_%S")

print(timestamp_str)

# COMMAND ----------

# MAGIC %md ### Write the raw data into a Unity Catalog Volume as a JSON File

# COMMAND ----------

dbutils.fs.put(f"/Volumes/gshen_catalog/aeso_demo/landing_zone/{timestamp_str}.json",json_string, True)
