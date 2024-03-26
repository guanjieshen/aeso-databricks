-- Databricks notebook source
-- MAGIC %md ### Ingest the raw JSON into a Delta table.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE pool_price_raw_dlt
AS SELECT *, current_timestamp() as ingest_timestamp FROM cloud_files("/Volumes/gshen_catalog/aeso_demo/landing_zone/", "json")

-- COMMAND ----------

-- MAGIC %md ### Convert the columns to the correct type, and add basic data quality checks.

-- COMMAND ----------

CREATE
OR REFRESH STREAMING TABLE pool_price_bronze_dlt(
-- Here we can add data quality checks to verify the dataset
  CONSTRAINT required_cols_not_null EXPECT (
    begin_datetime_mpt is not null
    and begin_datetime_utc is not null
    and forecast_pool_price is not null
  )
) AS
SELECT
  timestamp_millis(cast(begin_datetime_mpt as long)) as begin_datetime_mpt,
  timestamp_millis(cast(begin_datetime_utc as long)) as begin_datetime_utc,
  cast(forecast_pool_price as double),
  cast(pool_price as double),
  cast(rolling_30day_avg as double),
  ingest_timestamp
FROM
  stream(live.pool_price_raw_dlt)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE pool_price_silver_dlt;

-- COMMAND ----------

APPLY CHANGES INTO
  live.pool_price_silver_dlt
FROM
  stream(live.pool_price_bronze_dlt)
KEYS
  (begin_datetime_mpt, begin_datetime_utc)
SEQUENCE BY
  ingest_timestamp
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

CREATE 
