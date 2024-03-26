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

asset_list= aeso_instance.get_asset_list()
pdf = pd.DataFrame(asset_list)
asset_df=spark.createDataFrame(pdf) 
display(asset_df)


# COMMAND ----------

# MAGIC %md ### Get AESO Pool Participant List
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime, timedelta
import pytz
import pandas as pd

# Get list of Unique Pool Participant Ids
col = "pool_participant_ID"
pool_participant_IDs = [row[col] for row in asset_df.select(col).distinct().collect()]


# Chunk to 20 value blocks (max # of IDs per API call)
chunk_size = 20
participant_chunks = []
while len(pool_participant_IDs) > 0:
  participant_chunks.append(pool_participant_IDs[:chunk_size])
  pool_participant_IDs = pool_participant_IDs[chunk_size:]

# Get Current Date, and Current Date - 1
utc_time = pytz.utc.localize(datetime.utcnow())
calgary_time = utc_time.astimezone(pytz.timezone("America/Edmonton"))

yesterday_date = (calgary_time + timedelta(days=-1)).strftime("%Y-%m-%d")
current_date = calgary_time.strftime("%Y-%m-%d")

print(f"Yesterday Date: {yesterday_date}")
print(f"Current Date: {current_date}")
print(f"Number of Participant Chunks {len(participant_chunks)}")

# COMMAND ----------

import concurrent.futures
import pandas as pd
from pyspark.sql import SparkSession

# Assuming aeso_instance and other necessary imports and variables are defined
def fetch_data(participant_chunk):
    separator = ","
    query_string = separator.join(str(x) for x in participant_chunk)
    participant_list = aeso_instance.get_metered_volume(
        start_date=yesterday_date,
        end_date=current_date,
        pool_participant_id=query_string,
    )
    pdf = pd.DataFrame(participant_list)
    participant_df = spark.createDataFrame(pdf)
    print(f"Fetching: {participant_chunk}")
    return participant_df

# Assuming participant_chunks is a list of chunks

dfs = []

# Using ThreadPoolExecutor for parallel execution
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    # Execute fetch_data function in parallel
    future_to_chunk = {executor.submit(fetch_data, chunk): chunk for chunk in participant_chunks}
    for future in concurrent.futures.as_completed(future_to_chunk):
        chunk = future_to_chunk[future]
        try:
            participant_df = future.result()
            dfs.append(participant_df)
        except Exception as exc:
            print(f'{chunk} generated an exception: {exc}')

# dfs now contains all the DataFrames

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

if dfs:
    all_dfs = dfs[0]
    for df in dfs[1:]:
        all_dfs = all_dfs.union(df)

    all_dfs.withColumn("ingestion_time", current_timestamp()).write.mode("append").saveAsTable("gshen_catalog.aeso.metered_volume_raw")
else:
    print("No Data")
