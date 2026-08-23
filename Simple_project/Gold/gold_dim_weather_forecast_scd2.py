# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Include city column in dim_weather_forecast
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS weather_forcast.gold.dim_weather_forecast
# MAGIC (
# MAGIC location_id BIGINT,
# MAGIC city STRING,
# MAGIC forecast_date DATE,
# MAGIC weather_code INT,
# MAGIC max_temp_c DOUBLE,
# MAGIC min_temp_c DOUBLE,
# MAGIC effective_from TIMESTAMP,
# MAGIC effective_to TIMESTAMP,
# MAGIC is_current BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Update merge to include city column
from pyspark.sql import functions as F

silver = spark.table("weather_forcast.silver.daily_weather")
location = spark.table("weather_forcast.gold.dim_location")

source = (
    silver.join(location,["city","country"])
    .select(
        "location_id",
        "city",
        "forecast_date",
        "weather_code",
        "max_temp_c",
        "min_temp_c"
    )
)

source.createOrReplaceTempView("forecast_updates")

spark.sql("""
MERGE INTO weather_forcast.gold.dim_weather_forecast t
USING forecast_updates s
ON t.location_id=s.location_id
AND t.city=s.city
AND t.forecast_date=s.forecast_date
AND t.is_current=true

WHEN MATCHED AND (
t.max_temp_c<>s.max_temp_c
OR t.min_temp_c<>s.min_temp_c
OR t.weather_code<>s.weather_code
)
THEN UPDATE SET
effective_to=current_timestamp(),
is_current=false

WHEN NOT MATCHED
THEN INSERT(
location_id,
city,
forecast_date,
weather_code,
max_temp_c,
min_temp_c,
effective_from,
effective_to,
is_current
)
VALUES(
s.location_id,
s.city,
s.forecast_date,
s.weather_code,
s.max_temp_c,
s.min_temp_c,
current_timestamp(),
NULL,
true
)
""")

display(spark.table("weather_forcast.gold.dim_weather_forecast"))