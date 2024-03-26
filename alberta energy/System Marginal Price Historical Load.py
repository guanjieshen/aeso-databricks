# Databricks notebook source
# MAGIC %md ### Get AESO Pool Price Data
# MAGIC Retrieve data from API

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

start_date = '2000-01-01'
end_date = '2023-01-01'
result = generate_dates(start_date, end_date)
result.reverse()
print(result[0:2])

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

start_year = 2023
end_year = 2023
result = generate_monthly_dates(start_year, end_year)
result.reverse()
for start_date, end_date in result:
    print(f"Start: {start_date}, End: {end_date}")

# COMMAND ----------

import pandas as pd
from aeso_api import aeso

aeso_api_key = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJvaXNrM2EiLCJpYXQiOjE2Nzc2OTMyOTh9.wojWs2r9ecH88GbPpy34FShwhkjXPJXBAijY8S3rNAw'
aeso_instance = aeso(aeso_api_key)

for start_date, end_date in result:
    print(f"Start: {start_date}, End: {end_date}")
    resp= aeso_instance.get_system_marginal_price(start_date=start_date,end_date=end_date)

    pdf = pd.DataFrame(resp)
    sparkDF=spark.createDataFrame(pdf) 

    sparkDF.write.mode("append").format("delta").saveAsTable("gshen_catalog.aeso.marginal_price_historical")
    
