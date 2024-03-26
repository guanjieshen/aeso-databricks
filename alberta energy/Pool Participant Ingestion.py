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

# Get list of Unique Pool Participant Ids
col = "pool_participant_ID"
pool_participant_IDs = [row[col] for row in asset_df.select(col).distinct().collect()]

# Chunk to 20 value blocks (max # of IDs per API call)
chunk_size = 20
chunks = []
while len(pool_participant_IDs) > 0:
  chunks.append(pool_participant_IDs[:chunk_size])
  pool_participant_IDs = pool_participant_IDs[chunk_size:]

# COMMAND ----------

import concurrent.futures
import pandas as pd
from pyspark.sql import SparkSession

# Assuming aeso_instance and other necessary imports and variables are defined

def fetch_and_process_data(participant_chunk):
    separator = ","
    query_string = separator.join(str(x) for x in participant_chunk)
    participant_list = aeso_instance.get_pool_participant_list(query_string)
    pdf = pd.DataFrame(participant_list)
    participant_df = spark.createDataFrame(pdf)
    print(f"Fetching: {participant_chunk}")
    return participant_df

# Assuming chunks is a list of participant chunks

participant_dfs = []

# Using ThreadPoolExecutor for parallel execution
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    # Submit tasks to the executor
    futures = [executor.submit(fetch_and_process_data, chunk) for chunk in chunks]
    
    # Retrieve results as they are completed
    for future in concurrent.futures.as_completed(futures):
        participant_df = future.result()
        participant_dfs.append(participant_df)

# participant_dfs now contains all the DataFrames


# COMMAND ----------

all_participants_df = participant_dfs[0]
for df in participant_dfs[1:]:
    all_participants_df = all_participants_df.union(df)

all_participants_df.write.mode("overwrite").saveAsTable("gshen_catalog.aeso.pool_participants_raw")
