# Databricks notebook source
# MAGIC %md ### Get AESO Pool Price Data
# MAGIC Retrieve data from API

# COMMAND ----------

# MAGIC %md ### Load Days

# COMMAND ----------

from datetime import datetime, timedelta

def generate_dates(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

    dates = []
    current_date = start_date

    while current_date <= end_date:
        dates.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)

    return dates

start_date = '2024-01-01'
end_date = '2024-01-26'
day_result = generate_dates(start_date, end_date)
day_result.reverse()
print(day_result)

# COMMAND ----------

import pandas as pd
from aeso_api import aeso

aeso_api_key = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJvaXNrM2EiLCJpYXQiOjE2Nzc2OTMyOTh9.wojWs2r9ecH88GbPpy34FShwhkjXPJXBAijY8S3rNAw'
aeso_instance = aeso(aeso_api_key)

for date in day_result:
    print(f"Start: {date}, End: {date}")
    resp= aeso_instance.get_pool_price_report(start_date=date,end_date=date)

    pdf = pd.DataFrame(resp)
    sparkDF=spark.createDataFrame(pdf) 

    sparkDF.write.mode("append").format("delta").saveAsTable("gshen_catalog.aeso.pool_price_historical")

# COMMAND ----------

# MAGIC %md ### Load Months

# COMMAND ----------

from datetime import datetime, timedelta
from calendar import monthrange

def generate_monthly_dates(start_year, end_year):
    dates = []

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            _, last_day = monthrange(year, month)
            start_date = datetime(year, month, 1).strftime('%Y-%m-%d')
            end_date = datetime(year, month, last_day).strftime('%Y-%m-%d')
            dates.append((start_date, end_date))

    return dates

start_year = 2000
end_year = 2023
month_result = generate_monthly_dates(start_year, end_year)
month_result.reverse()
for start_date, end_date in month_result :
    print(f"Start: {start_date}, End: {end_date}")

# COMMAND ----------

import pandas as pd
from aeso_api import aeso

aeso_api_key = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJvaXNrM2EiLCJpYXQiOjE2Nzc2OTMyOTh9.wojWs2r9ecH88GbPpy34FShwhkjXPJXBAijY8S3rNAw'
aeso_instance = aeso(aeso_api_key)

for start_date, end_date in month_result:
    print(f"Start: {start_date}, End: {end_date}")
    resp= aeso_instance.get_pool_price_report(start_date=start_date,end_date=end_date)

    pdf = pd.DataFrame(resp)
    sparkDF=spark.createDataFrame(pdf) 

    sparkDF.write.mode("append").format("delta").saveAsTable("gshen_catalog.aeso.pool_price_historical")
    

# COMMAND ----------

# MAGIC %md ### Remove Duplicates

# COMMAND ----------

# MAGIC %pip install mack

# COMMAND ----------

from delta.tables import DeltaTable
history_table = DeltaTable.forName(spark,"gshen_catalog.aeso.pool_price_historical")

# COMMAND ----------

import mack

mack.kill_duplicates(
    history_table,
    [
        "begin_datetime_utc",
        "begin_datetime_mpt",
        "pool_price",
        "forecast_pool_price",
        "rolling_30day_avg",
    ],
)

# COMMAND ----------

# MAGIC %md ### Merge

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gshen_catalog.aeso.pool_price_raw as target USING gshen_catalog.aeso.pool_price_historical as source ON target.begin_datetime_mpt = source.begin_datetime_mpt
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE
# MAGIC SET
# MAGIC   target.pool_price = source.pool_price,
# MAGIC   target.rolling_30day_avg = source.rolling_30day_avg,
# MAGIC   target.forecast_pool_price = source.forecast_pool_price
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   *
