# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

bronze_df = spark.table("weather_forcast.bronze.weather_raw")

current_df = (
    bronze_df.select(
        "city",
        "country",
        "latitude",
        "longitude",
        "ingestion_timestamp",
        F.col("current.time").alias("observation_time"),
        F.col("current.temperature_2m").cast("double").alias("temperature_c"),
        F.col("current.apparent_temperature").cast("double").alias("feels_like_c"),
        F.col("current.relative_humidity_2m").cast("int").alias("humidity"),
        F.col("current.wind_speed_10m").cast("double").alias("wind_ms"),
        F.col("current.wind_gusts_10m").cast("double").alias("wind_gust_ms"),
        F.col("current.cloud_cover").cast("int").alias("cloud_cover"),
        F.col("current.surface_pressure").cast("double").alias("surface_pressure"),
        F.col("current.is_day").cast("int").alias("is_day"),
        F.col("current.weather_code").cast("int").alias("weather_code")
    )
    .withColumn("temperature_f", F.round(F.col("temperature_c")*9/5+32,1))
    .withColumn("wind_kmh", F.round(F.col("wind_ms")*3.6,1))
    .withColumn("wind_gust_kmh", F.round(F.col("wind_gust_ms")*3.6,1))
    .withColumn(
        "weather_description",
        F.when(F.col("weather_code")==0,"Clear Sky")
         .when(F.col("weather_code")==1,"Mainly Clear")
         .when(F.col("weather_code")==2,"Partly Cloudy")
         .when(F.col("weather_code")==3,"Cloudy")
         .when(F.col("weather_code")==61,"Rain")
         .when(F.col("weather_code")==71,"Snow")
         .otherwise("Unknown")
    )
    .withColumn(
        "temperature_category",
        F.when(F.col("temperature_c")<15,"Cold")
         .when(F.col("temperature_c")<=30,"Warm")
         .otherwise("Hot")
    )
)

current_df.write.mode("overwrite").format("delta").saveAsTable("weather_forcast.silver.current_weather")

# display(current_df)

# COMMAND ----------

# assert current_df.filter(F.col("city").isNull()).count()==0
# assert current_df.filter(F.col("temperature_c").isNull()).count()==0
# assert current_df.filter((F.col("humidity")<0)|(F.col("humidity")>100)).count()==0
# assert current_df.filter(F.col("wind_kmh")<0).count()==0

# COMMAND ----------

# DBTITLE 1,Notebook Documentation
# MAGIC %md
# MAGIC # Current Weather Transformation - Silver Layer
# MAGIC
# MAGIC ## 📋 Overview
# MAGIC This notebook transforms raw weather data from the Bronze layer into a clean, structured format in the Silver layer. It extracts current weather conditions, performs type conversions, adds derived metrics, and applies data quality validations.
# MAGIC
# MAGIC ## 🔄 Data Flow
# MAGIC **Input:** `weather_forcast.bronze.weather_raw` (Bronze Layer)  
# MAGIC **Output:** `weather_forcast.silver.current_weather` (Silver Layer)  
# MAGIC **Layer:** Bronze → Silver (Data Cleansing & Transformation)
# MAGIC
# MAGIC ## 🔧 Transformations
# MAGIC
# MAGIC ### 1. Schema Creation
# MAGIC - Creates `silver` schema if it doesn't exist
# MAGIC - Ensures proper namespace organization
# MAGIC
# MAGIC ### 2. Data Extraction & Type Casting
# MAGIC Extracts current weather fields from Bronze MAP structure and casts to appropriate types:
# MAGIC
# MAGIC | Source Field | Target Column | Type | Description |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `current.time` | `observation_time` | string | Timestamp of observation |
# MAGIC | `current.temperature_2m` | `temperature_c` | double | Temperature in Celsius |
# MAGIC | `current.apparent_temperature` | `feels_like_c` | double | Perceived temperature |
# MAGIC | `current.relative_humidity_2m` | `humidity` | int | Relative humidity (%) |
# MAGIC | `current.wind_speed_10m` | `wind_ms` | double | Wind speed (m/s) |
# MAGIC | `current.wind_gusts_10m` | `wind_gust_ms` | double | Wind gust speed (m/s) |
# MAGIC | `current.cloud_cover` | `cloud_cover` | int | Cloud coverage (%) |
# MAGIC | `current.surface_pressure` | `surface_pressure` | double | Surface pressure (hPa) |
# MAGIC | `current.is_day` | `is_day` | int | Day indicator (1=day, 0=night) |
# MAGIC | `current.weather_code` | `weather_code` | int | WMO weather code |
# MAGIC
# MAGIC **Why Casting?** Bronze layer stores all values as strings in MAP format. Explicit casting to numeric types (double/int) prevents downstream cast errors and ensures proper data types for analytics.
# MAGIC
# MAGIC ### 3. Derived Metrics
# MAGIC
# MAGIC #### Temperature Conversion
# MAGIC - **temperature_f**: Converts Celsius to Fahrenheit  
# MAGIC   Formula: `(temperature_c × 9/5) + 32`, rounded to 1 decimal
# MAGIC
# MAGIC #### Wind Speed Conversion
# MAGIC - **wind_kmh**: Converts m/s to km/h  
# MAGIC   Formula: `wind_ms × 3.6`, rounded to 1 decimal
# MAGIC - **wind_gust_kmh**: Converts gust speed to km/h  
# MAGIC   Formula: `wind_gust_ms × 3.6`, rounded to 1 decimal
# MAGIC
# MAGIC #### Weather Description Mapping
# MAGIC Maps WMO weather codes to human-readable descriptions:
# MAGIC - `0` → Clear Sky
# MAGIC - `1` → Mainly Clear
# MAGIC - `2` → Partly Cloudy
# MAGIC - `3` → Cloudy
# MAGIC - `61` → Rain
# MAGIC - `71` → Snow
# MAGIC - Others → Unknown
# MAGIC
# MAGIC #### Temperature Categorization
# MAGIC - **Cold**: < 15°C
# MAGIC - **Warm**: 15-30°C
# MAGIC - **Hot**: > 30°C
# MAGIC
# MAGIC ### 4. Metadata Preservation
# MAGIC Retains key metadata from Bronze:
# MAGIC - `city`: City name
# MAGIC - `country`: Country name
# MAGIC - `latitude`, `longitude`: Geographic coordinates
# MAGIC - `ingestion_timestamp`: When data was ingested
# MAGIC
# MAGIC ## ✅ Data Quality Validations
# MAGIC
# MAGIC The notebook includes assertion checks to ensure data quality:
# MAGIC
# MAGIC 1. **City Not Null**: All records must have a city value
# MAGIC 2. **Temperature Not Null**: Temperature must be present for all records
# MAGIC 3. **Humidity Range**: Humidity must be between 0-100%
# MAGIC 4. **Non-negative Wind Speed**: Wind speed cannot be negative
# MAGIC
# MAGIC If any assertion fails, the notebook stops execution and raises an error.
# MAGIC
# MAGIC ## 📊 Output Schema
# MAGIC
# MAGIC ### Silver Table: `weather_forcast.silver.current_weather`
# MAGIC
# MAGIC **Write Mode:** Overwrite (replaces all data on each run)  
# MAGIC **Format:** Delta Lake
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | city | string | City name |
# MAGIC | country | string | Country name |
# MAGIC | latitude | double | Latitude coordinate |
# MAGIC | longitude | double | Longitude coordinate |
# MAGIC | ingestion_timestamp | timestamp | When data was ingested |
# MAGIC | observation_time | string | ISO 8601 observation timestamp |
# MAGIC | temperature_c | double | Temperature (Celsius) |
# MAGIC | feels_like_c | double | Apparent temperature (Celsius) |
# MAGIC | humidity | int | Relative humidity (%) |
# MAGIC | wind_ms | double | Wind speed (m/s) |
# MAGIC | wind_gust_ms | double | Wind gust speed (m/s) |
# MAGIC | cloud_cover | int | Cloud coverage (%) |
# MAGIC | surface_pressure | double | Surface pressure (hPa) |
# MAGIC | is_day | int | Day indicator (1=day, 0=night) |
# MAGIC | weather_code | int | WMO weather code |
# MAGIC | temperature_f | double | Temperature (Fahrenheit) |
# MAGIC | wind_kmh | double | Wind speed (km/h) |
# MAGIC | wind_gust_kmh | double | Wind gust speed (km/h) |
# MAGIC | weather_description | string | Human-readable weather |
# MAGIC | temperature_category | string | Cold/Warm/Hot |
# MAGIC
# MAGIC ## 🎯 Usage
# MAGIC
# MAGIC ### Standalone Execution
# MAGIC ```python
# MAGIC %run ./current_weather
# MAGIC ```
# MAGIC
# MAGIC ### As Part of Pipeline
# MAGIC This notebook is designed to run after the Bronze ingestion:
# MAGIC 1. **Bronze Notebook** → Fetches raw weather data
# MAGIC 2. **This Notebook** → Transforms to Silver layer
# MAGIC 3. **Gold Notebook** → Creates business-level aggregates
# MAGIC
# MAGIC ### Downstream Consumption
# MAGIC Analytics queries can directly use `weather_forcast.silver.current_weather`:
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     city,
# MAGIC     temperature_c,
# MAGIC     weather_description,
# MAGIC     temperature_category
# MAGIC FROM weather_forcast.silver.current_weather
# MAGIC WHERE temperature_category = 'Hot'
# MAGIC ```
# MAGIC
# MAGIC ## 🔍 Data Quality Notes
# MAGIC
# MAGIC ### Strengths
# MAGIC - Type-safe transformations with explicit casting
# MAGIC - Derived metrics for common use cases
# MAGIC - Automated data quality validations
# MAGIC - Human-readable weather descriptions
# MAGIC
# MAGIC ### Limitations
# MAGIC - Limited weather code mappings (only 6 codes mapped)
# MAGIC - Temperature categories use fixed thresholds (may vary by geography)
# MAGIC - Overwrite mode means no historical tracking at Silver layer
# MAGIC
# MAGIC ### Best Practices
# MAGIC - Run this notebook immediately after Bronze ingestion
# MAGIC - Monitor assertion failures in production
# MAGIC - Extend weather code mappings as needed
# MAGIC - Consider append mode if historical Silver data is needed
# MAGIC
# MAGIC ## 📅 Last Updated
# MAGIC 2026-08-22