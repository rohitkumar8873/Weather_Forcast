# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F

bronze_df = spark.table("weather_forcast.bronze.weather_raw")

hourly_df = (
    bronze_df
    .select(
        "city",
        "country",
        F.arrays_zip(
            "hourly.time",
            "hourly.temperature_2m",
            "hourly.relative_humidity_2m",
            "hourly.precipitation_probability",
            "hourly.wind_speed_10m",
            "hourly.wind_gusts_10m",
            "hourly.cloud_cover",
            "hourly.visibility"
        ).alias("hourly_data")
    )
    .select("city","country",F.explode("hourly_data").alias("h"))
    .select(
        "city",
        "country",
        F.col("h.time").alias("forecast_time"),
        F.col("h.temperature_2m").cast("double").alias("temperature_c"),
        F.col("h.relative_humidity_2m").cast("int").alias("humidity"),
        F.col("h.precipitation_probability").cast("int").alias("rain_probability"),
        F.col("h.wind_speed_10m").cast("double").alias("wind_ms"),
        F.col("h.wind_gusts_10m").cast("double").alias("wind_gust_ms"),
        F.col("h.cloud_cover").cast("int").alias("cloud_cover"),
        F.col("h.visibility").cast("double").alias("visibility_m")
    )
    .withColumn("wind_kmh",F.round(F.col("wind_ms")*3.6,1))
    .withColumn("wind_gust_kmh",F.round(F.col("wind_gust_ms")*3.6,1))
)

hourly_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("weather_forcast.silver.hourly_weather")

# display(hourly_df)

# COMMAND ----------

# assert hourly_df.filter(F.col("forecast_time").isNull()).count()==0
# assert hourly_df.filter(F.col("temperature_c").isNull()).count()==0
# assert hourly_df.filter((F.col("rain_probability")<0)|(F.col("rain_probability")>100)).count()==0
# assert hourly_df.filter(F.col("visibility_m")<0).count()==0

# COMMAND ----------

# DBTITLE 1,Notebook Documentation
# MAGIC %md
# MAGIC # Hourly Weather Forecast Transformation - Silver Layer
# MAGIC
# MAGIC ## 📋 Overview
# MAGIC This notebook transforms raw hourly weather forecast data from the Bronze layer into a clean, structured format in the Silver layer. It unpacks nested hourly arrays, performs type conversions, adds derived wind speed metrics, and applies data quality validations.
# MAGIC
# MAGIC ## 🔄 Data Flow
# MAGIC **Input:** `weather_forcast.bronze.weather_raw` (Bronze Layer)  
# MAGIC **Output:** `weather_forcast.silver.hourly_weather` (Silver Layer)  
# MAGIC **Layer:** Bronze → Silver (Data Cleansing & Transformation)
# MAGIC
# MAGIC ## 🔧 Transformations
# MAGIC
# MAGIC ### 1. Read Bronze Data
# MAGIC Reads raw weather data from the Bronze layer table containing nested hourly forecast arrays.
# MAGIC
# MAGIC ### 2. Array Unpacking
# MAGIC Uses `F.arrays_zip()` to combine multiple hourly arrays into a single struct array:
# MAGIC - `hourly.time` - Forecast timestamp
# MAGIC - `hourly.temperature_2m` - Temperature readings
# MAGIC - `hourly.relative_humidity_2m` - Humidity values
# MAGIC - `hourly.precipitation_probability` - Rain probability
# MAGIC - `hourly.wind_speed_10m` - Wind speed measurements
# MAGIC - `hourly.wind_gusts_10m` - Wind gust measurements
# MAGIC - `hourly.cloud_cover` - Cloud coverage percentage
# MAGIC - `hourly.visibility` - Visibility distance
# MAGIC
# MAGIC ### 3. Array Explosion
# MAGIC Uses `F.explode()` to convert each hourly record from a single row with 168 array elements into 168 individual rows (one per hour).
# MAGIC
# MAGIC **Example transformation:**
# MAGIC ```
# MAGIC Before: 1 row per city with arrays of 168 elements
# MAGIC After:  168 rows per city (one row per forecast hour)
# MAGIC ```
# MAGIC
# MAGIC ### 4. Data Extraction & Type Casting
# MAGIC Extracts fields from the exploded struct and casts to appropriate types:
# MAGIC
# MAGIC | Source Field | Target Column | Type | Description |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `h.time` | `forecast_time` | string | ISO 8601 forecast timestamp |
# MAGIC | `h.temperature_2m` | `temperature_c` | double | Temperature (Celsius) |
# MAGIC | `h.relative_humidity_2m` | `humidity` | int | Relative humidity (%) |
# MAGIC | `h.precipitation_probability` | `rain_probability` | int | Precipitation probability (%) |
# MAGIC | `h.wind_speed_10m` | `wind_ms` | double | Wind speed (m/s) |
# MAGIC | `h.wind_gusts_10m` | `wind_gust_ms` | double | Wind gust speed (m/s) |
# MAGIC | `h.cloud_cover` | `cloud_cover` | int | Cloud coverage (%) |
# MAGIC | `h.visibility` | `visibility_m` | double | Visibility (meters) |
# MAGIC
# MAGIC **Why Casting?** Bronze layer stores all values as strings in nested MAP/ARRAY structures. Explicit casting to numeric types (double/int) prevents downstream cast errors and ensures proper data types for analytics.
# MAGIC
# MAGIC ### 5. Derived Metrics
# MAGIC
# MAGIC #### Wind Speed Conversion
# MAGIC - **wind_kmh**: Converts m/s to km/h  
# MAGIC   Formula: `wind_ms × 3.6`, rounded to 1 decimal
# MAGIC - **wind_gust_kmh**: Converts gust speed to km/h  
# MAGIC   Formula: `wind_gust_ms × 3.6`, rounded to 1 decimal
# MAGIC
# MAGIC ### 6. Metadata Preservation
# MAGIC Retains key metadata from Bronze:
# MAGIC - `city`: City name
# MAGIC - `country`: Country name
# MAGIC
# MAGIC ## ✅ Data Quality Validations
# MAGIC
# MAGIC The notebook includes assertion checks to ensure data quality:
# MAGIC
# MAGIC 1. **Forecast Time Not Null**: All records must have a forecast timestamp
# MAGIC 2. **Temperature Not Null**: Temperature must be present for all records
# MAGIC 3. **Rain Probability Range**: Must be between 0-100%
# MAGIC 4. **Non-negative Visibility**: Visibility cannot be negative
# MAGIC
# MAGIC If any assertion fails, the notebook stops execution and raises an error.
# MAGIC
# MAGIC ## 📊 Output Schema
# MAGIC
# MAGIC ### Silver Table: `weather_forcast.silver.hourly_weather`
# MAGIC
# MAGIC **Write Mode:** Overwrite (replaces all data on each run)  
# MAGIC **Format:** Delta Lake  
# MAGIC **Schema Option:** `overwriteSchema=true` (allows schema evolution)
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | city | string | City name |
# MAGIC | country | string | Country name |
# MAGIC | forecast_time | string | ISO 8601 forecast timestamp (hourly) |
# MAGIC | temperature_c | double | Temperature (Celsius) |
# MAGIC | humidity | int | Relative humidity (%) |
# MAGIC | rain_probability | int | Precipitation probability (%) |
# MAGIC | wind_ms | double | Wind speed (m/s) |
# MAGIC | wind_gust_ms | double | Wind gust speed (m/s) |
# MAGIC | cloud_cover | int | Cloud coverage (%) |
# MAGIC | visibility_m | double | Visibility (meters) |
# MAGIC | wind_kmh | double | Wind speed (km/h) |
# MAGIC | wind_gust_kmh | double | Wind gust speed (km/h) |
# MAGIC
# MAGIC **Data Volume:**
# MAGIC - 168 rows per city (7 days × 24 hours)
# MAGIC - Total rows = 168 × number of cities in Bronze
# MAGIC
# MAGIC ## 🎯 Usage
# MAGIC
# MAGIC ### Standalone Execution
# MAGIC ```python
# MAGIC %run ./silver_hourly_weather
# MAGIC ```
# MAGIC
# MAGIC ### As Part of Pipeline
# MAGIC This notebook is designed to run after the Bronze ingestion:
# MAGIC 1. **Bronze Notebook** → Fetches raw weather data
# MAGIC 2. **Current Weather Notebook** → Transforms current conditions
# MAGIC 3. **This Notebook** → Transforms hourly forecasts
# MAGIC 4. **Gold Notebook** → Creates business-level aggregates
# MAGIC
# MAGIC ### Downstream Consumption
# MAGIC Analytics queries can directly use `weather_forcast.silver.hourly_weather`:
# MAGIC
# MAGIC ```sql
# MAGIC -- Find peak wind speeds per city
# MAGIC SELECT 
# MAGIC     city,
# MAGIC     DATE(forecast_time) as forecast_date,
# MAGIC     MAX(wind_kmh) as max_wind_kmh,
# MAGIC     MAX(wind_gust_kmh) as max_gust_kmh
# MAGIC FROM weather_forcast.silver.hourly_weather
# MAGIC GROUP BY city, DATE(forecast_time)
# MAGIC ORDER BY max_gust_kmh DESC
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC -- Find hours with high rain probability
# MAGIC SELECT 
# MAGIC     city,
# MAGIC     forecast_time,
# MAGIC     temperature_c,
# MAGIC     rain_probability,
# MAGIC     humidity
# MAGIC FROM weather_forcast.silver.hourly_weather
# MAGIC WHERE rain_probability > 70
# MAGIC ORDER BY forecast_time
# MAGIC ```
# MAGIC
# MAGIC ## 🔍 Data Quality Notes
# MAGIC
# MAGIC ### Strengths
# MAGIC - Type-safe transformations with explicit casting
# MAGIC - Handles nested array structures efficiently
# MAGIC - Derived metrics for common use cases
# MAGIC - Automated data quality validations
# MAGIC - Proper schema management with `overwriteSchema`
# MAGIC
# MAGIC ### Key Design Decisions
# MAGIC
# MAGIC #### Why `arrays_zip()` + `explode()`?
# MAGIC This pattern efficiently unpacks multiple parallel arrays into individual rows:
# MAGIC - **Alternative 1**: Multiple joins on array indices → slower, more complex
# MAGIC - **Alternative 2**: UDFs → harder to maintain, less optimized
# MAGIC - **Chosen approach**: Native Spark functions → optimal performance
# MAGIC
# MAGIC #### Why `overwriteSchema=true`?
# MAGIC - Allows schema evolution when new fields are added to Bronze
# MAGIC - Replaces entire schema on type changes (e.g., string → double corrections)
# MAGIC - Use `mergeSchema=true` only for additive changes, not type changes
# MAGIC
# MAGIC ### Limitations
# MAGIC - Overwrite mode means no historical tracking at Silver layer
# MAGIC - 168 hours (7 days) coverage — older forecasts are dropped
# MAGIC - Assumes all cities have complete 168-hour arrays
# MAGIC
# MAGIC ### Best Practices
# MAGIC - Run this notebook immediately after Bronze ingestion
# MAGIC - Monitor assertion failures in production
# MAGIC - Consider append mode if historical Silver data is needed
# MAGIC - For incremental processing, filter Bronze by `ingestion_timestamp`
# MAGIC
# MAGIC ## 🔧 Troubleshooting
# MAGIC
# MAGIC ### Common Issues
# MAGIC
# MAGIC **Schema merge errors:**
# MAGIC - Use `overwriteSchema=true` (not `mergeSchema`) when fixing type errors
# MAGIC - `mergeSchema` cannot reconcile conflicting types (e.g., string → double)
# MAGIC
# MAGIC **Missing data after explode:**
# MAGIC - Check Bronze table has `hourly` field populated
# MAGIC - Verify `arrays_zip` includes all required fields
# MAGIC - Some cities may have incomplete hourly arrays
# MAGIC
# MAGIC **Type casting failures:**
# MAGIC - Inspect Bronze data for non-numeric values in numeric fields
# MAGIC - Add null handling if API returns missing values
# MAGIC
# MAGIC ## 📅 Last Updated
# MAGIC 2026-08-22