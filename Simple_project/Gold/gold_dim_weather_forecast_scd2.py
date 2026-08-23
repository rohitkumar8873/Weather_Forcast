# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Include city column in dim_weather_forecast
# MAGIC %skip
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
# MAGIC %skip
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC silver = spark.table("weather_forcast.silver.daily_weather")
# MAGIC location = spark.table("weather_forcast.gold.dim_location")
# MAGIC
# MAGIC source = (
# MAGIC     silver.join(location,["city","country"])
# MAGIC     .select(
# MAGIC         "location_id",
# MAGIC         "city",
# MAGIC         "forecast_date",
# MAGIC         "weather_code",
# MAGIC         "max_temp_c",
# MAGIC         "min_temp_c"
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC source.createOrReplaceTempView("forecast_updates")
# MAGIC
# MAGIC spark.sql("""
# MAGIC MERGE INTO weather_forcast.gold.dim_weather_forecast t
# MAGIC USING forecast_updates s
# MAGIC ON t.location_id=s.location_id
# MAGIC AND t.city=s.city
# MAGIC AND t.forecast_date=s.forecast_date
# MAGIC AND t.is_current=true
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC t.max_temp_c<>s.max_temp_c
# MAGIC OR t.min_temp_c<>s.min_temp_c
# MAGIC OR t.weather_code<>s.weather_code
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC effective_to=current_timestamp(),
# MAGIC is_current=false
# MAGIC
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT(
# MAGIC location_id,
# MAGIC city,
# MAGIC forecast_date,
# MAGIC weather_code,
# MAGIC max_temp_c,
# MAGIC min_temp_c,
# MAGIC effective_from,
# MAGIC effective_to,
# MAGIC is_current
# MAGIC )
# MAGIC VALUES(
# MAGIC s.location_id,
# MAGIC s.city,
# MAGIC s.forecast_date,
# MAGIC s.weather_code,
# MAGIC s.max_temp_c,
# MAGIC s.min_temp_c,
# MAGIC current_timestamp(),
# MAGIC NULL,
# MAGIC true
# MAGIC )
# MAGIC """)
# MAGIC
# MAGIC display(spark.table("weather_forcast.gold.dim_weather_forecast"))

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC drop table weather_forcast.gold.dim_weather_forecast

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS weather_forcast.gold.dim_weather_forecast
# MAGIC (
# MAGIC     location_id BIGINT,
# MAGIC     city STRING,
# MAGIC     snapshot_date DATE,          -- Forecast generation date (e.g., 22 Aug, 23 Aug)
# MAGIC     forecast_date DATE,          -- Forecasted date (e.g., 24 Aug)
# MAGIC     weather_code INT,
# MAGIC     max_temp_c DOUBLE,
# MAGIC     min_temp_c DOUBLE,
# MAGIC     effective_from TIMESTAMP,
# MAGIC     effective_to TIMESTAMP,
# MAGIC     is_current BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %skip
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC silver = spark.table("weather_forcast.silver.daily_weather")
# MAGIC location = spark.table("weather_forcast.gold.dim_location")
# MAGIC
# MAGIC source = (
# MAGIC     silver.join(location, ["city", "country"])
# MAGIC     .select(
# MAGIC         "location_id",
# MAGIC         "city",
# MAGIC         "forecast_date",
# MAGIC         "weather_code",
# MAGIC         "max_temp_c",
# MAGIC         "min_temp_c"
# MAGIC     )
# MAGIC     .withColumn("snapshot_date", F.current_date())
# MAGIC )
# MAGIC
# MAGIC source.createOrReplaceTempView("forecast_updates")

# COMMAND ----------

# MAGIC %skip
# MAGIC spark.sql("""
# MAGIC UPDATE weather_forcast.gold.dim_weather_forecast AS t
# MAGIC SET
# MAGIC     effective_to = current_timestamp(),
# MAGIC     is_current = false
# MAGIC WHERE t.is_current = true
# MAGIC   AND EXISTS (
# MAGIC       SELECT 1
# MAGIC       FROM forecast_updates s
# MAGIC       WHERE s.location_id = t.location_id
# MAGIC         AND s.city = t.city
# MAGIC   )
# MAGIC """)

# COMMAND ----------

# MAGIC %skip
# MAGIC spark.sql("""
# MAGIC UPDATE weather_forcast.gold.dim_weather_forecast
# MAGIC SET
# MAGIC     effective_to = current_timestamp(),
# MAGIC     is_current = false
# MAGIC WHERE is_current = true
# MAGIC """)

# COMMAND ----------

# MAGIC %skip
# MAGIC spark.sql("""
# MAGIC INSERT INTO weather_forcast.gold.dim_weather_forecast
# MAGIC (
# MAGIC     location_id,
# MAGIC     city,
# MAGIC     snapshot_date,
# MAGIC     forecast_date,
# MAGIC     weather_code,
# MAGIC     max_temp_c,
# MAGIC     min_temp_c,
# MAGIC     effective_from,
# MAGIC     effective_to,
# MAGIC     is_current
# MAGIC )
# MAGIC SELECT
# MAGIC     location_id,
# MAGIC     city,
# MAGIC     snapshot_date,
# MAGIC     forecast_date,
# MAGIC     weather_code,
# MAGIC     max_temp_c,
# MAGIC     min_temp_c,
# MAGIC     current_timestamp(),
# MAGIC     NULL,
# MAGIC     true
# MAGIC FROM forecast_updates
# MAGIC """)

# COMMAND ----------

# MAGIC %skip
# MAGIC display(
# MAGIC     spark.table("weather_forcast.gold.dim_weather_forecast")
# MAGIC          .orderBy(F.desc("snapshot_date"), "forecast_date")
# MAGIC )

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS weather_forcast.gold.dim_weather_forecast
# MAGIC (
# MAGIC     location_id BIGINT,
# MAGIC     city STRING,
# MAGIC     snapshot_date DATE,
# MAGIC     forecast_date DATE,
# MAGIC     weather_code INT,
# MAGIC     max_temp_c DOUBLE,
# MAGIC     min_temp_c DOUBLE,
# MAGIC     effective_from TIMESTAMP,
# MAGIC     effective_to TIMESTAMP,
# MAGIC     is_current BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# ---------------------------------------------------
# Step 1: Read Silver and Dimension tables
# ---------------------------------------------------

silver = spark.table("weather_forcast.silver.daily_weather")
location = spark.table("weather_forcast.gold.dim_location")

# COMMAND ----------

# ---------------------------------------------------
# Step 2: Create today's forecast snapshot
# ---------------------------------------------------

source = (
    silver.join(location, ["city", "country"])
    .select(
        "location_id",
        "city",
        "forecast_date",
        "weather_code",
        "max_temp_c",
        "min_temp_c"
    )
    .withColumn("snapshot_date", F.current_date())
)

source.createOrReplaceTempView("forecast_updates")


# COMMAND ----------

# ---------------------------------------------------
# Step 3: Close previous forecast window
# (Only for cities present in today's run)
# ---------------------------------------------------

spark.sql("""
UPDATE weather_forcast.gold.dim_weather_forecast AS t
SET
    effective_to = current_timestamp(),
    is_current = false
WHERE t.is_current = true
  AND EXISTS (
      SELECT 1
      FROM forecast_updates s
      WHERE s.location_id = t.location_id
        AND s.city = t.city
  )
""")

# COMMAND ----------

# ---------------------------------------------------
# Step 4: Insert today's forecast window
# ---------------------------------------------------

spark.sql("""
INSERT INTO weather_forcast.gold.dim_weather_forecast
(
    location_id,
    city,
    snapshot_date,
    forecast_date,
    weather_code,
    max_temp_c,
    min_temp_c,
    effective_from,
    effective_to,
    is_current
)
SELECT
    location_id,
    city,
    snapshot_date,
    forecast_date,
    weather_code,
    max_temp_c,
    min_temp_c,
    current_timestamp(),
    NULL,
    true
FROM forecast_updates
""")


# COMMAND ----------

# ---------------------------------------------------
# Step 5: Display SCD2 History
# ---------------------------------------------------

display(
    spark.table("weather_forcast.gold.dim_weather_forecast")
         .orderBy(F.col("city"), F.desc("snapshot_date"), F.col("forecast_date"))
)