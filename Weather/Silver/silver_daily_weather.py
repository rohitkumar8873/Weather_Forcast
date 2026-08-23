# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F

bronze_df = spark.table("weather_forcast.bronze.weather_raw")

daily_df = (
    bronze_df
    .select(
        "city",
        "country",
        F.arrays_zip(
            "daily.time",
            "daily.temperature_2m_max",
            "daily.temperature_2m_min",
            "daily.apparent_temperature_max",
            "daily.apparent_temperature_min",
            "daily.precipitation_sum",
            "daily.weather_code",
            "daily.sunrise",
            "daily.sunset",
            "daily.uv_index_max",
            "daily.wind_speed_10m_max",
            "daily.wind_gusts_10m_max",
            "daily.sunshine_duration"
        ).alias("daily_data")
    )
    .select("city","country",F.explode("daily_data").alias("d"))
    .select(
        "city",
        "country",
        F.col("d.time").alias("forecast_date"),
        F.col("d.temperature_2m_max").cast("double").alias("max_temp_c"),
        F.col("d.temperature_2m_min").cast("double").alias("min_temp_c"),
        F.col("d.apparent_temperature_max").cast("double").alias("feels_max_c"),
        F.col("d.apparent_temperature_min").cast("double").alias("feels_min_c"),
        F.col("d.precipitation_sum").cast("double").alias("rainfall_mm"),
        F.col("d.weather_code").cast("int").alias("weather_code"),
        F.col("d.sunrise").alias("sunrise"),
        F.col("d.sunset").alias("sunset"),
        F.col("d.uv_index_max").cast("double").alias("uv_index"),
        F.col("d.wind_speed_10m_max").cast("double").alias("wind_max_ms"),
        F.col("d.wind_gusts_10m_max").cast("double").alias("gust_max_ms"),
        F.col("d.sunshine_duration").cast("double").alias("sunshine_seconds")
    )
    .withColumn("day_name",F.date_format("forecast_date","EEEE"))
    .withColumn("is_weekend",F.dayofweek("forecast_date").isin([1,7]))
    .withColumn("sunshine_hours",F.round(F.col("sunshine_seconds")/3600,2))
    .withColumn("wind_max_kmh",F.round(F.col("wind_max_ms")*3.6,1))
    .withColumn("gust_max_kmh",F.round(F.col("gust_max_ms")*3.6,1))
)

daily_df.write.mode("overwrite").format("delta").saveAsTable("weather_forcast.silver.daily_weather")

display(daily_df)

# COMMAND ----------

assert daily_df.filter(F.col("forecast_date").isNull()).count()==0
assert daily_df.filter(F.col("max_temp_c")<F.col("min_temp_c")).count()==0
assert daily_df.filter(F.col("rainfall_mm")<0).count()==0
assert daily_df.filter((F.col("uv_index")<0)|(F.col("uv_index")>15)).count()==0
assert daily_df.filter(F.col("sunshine_hours")<0).count()==0

# COMMAND ----------

# DBTITLE 1,Notebook Documentation
# MAGIC %md
# MAGIC # Daily Weather Forecast Transformation - Silver Layer
# MAGIC
# MAGIC ## 📋 Overview
# MAGIC This notebook transforms raw daily weather forecast data from the Bronze layer into a clean, structured format in the Silver layer. It unpacks nested daily arrays, performs type conversions, adds derived calendar and weather metrics, and applies data quality validations.
# MAGIC
# MAGIC ## 🔄 Data Flow
# MAGIC **Input:** `weather_forcast.bronze.weather_raw` (Bronze Layer)  
# MAGIC **Output:** `weather_forcast.silver.daily_weather` (Silver Layer)  
# MAGIC **Layer:** Bronze → Silver (Data Cleansing & Transformation)
# MAGIC
# MAGIC ## 🔧 Transformations
# MAGIC
# MAGIC ### 1. Read Bronze Data
# MAGIC Reads raw weather data from the Bronze layer table containing nested daily forecast arrays.
# MAGIC
# MAGIC ### 2. Array Unpacking
# MAGIC Uses `F.arrays_zip()` to combine multiple daily arrays into a single struct array:
# MAGIC - `daily.time` - Forecast date
# MAGIC - `daily.temperature_2m_max` - Maximum temperature
# MAGIC - `daily.temperature_2m_min` - Minimum temperature
# MAGIC - `daily.apparent_temperature_max` - Maximum feels-like temperature
# MAGIC - `daily.apparent_temperature_min` - Minimum feels-like temperature
# MAGIC - `daily.precipitation_sum` - Total rainfall
# MAGIC - `daily.weather_code` - WMO weather code
# MAGIC - `daily.sunrise` - Sunrise time
# MAGIC - `daily.sunset` - Sunset time
# MAGIC - `daily.uv_index_max` - Maximum UV index
# MAGIC - `daily.wind_speed_10m_max` - Maximum wind speed
# MAGIC - `daily.wind_gusts_10m_max` - Maximum wind gust
# MAGIC - `daily.sunshine_duration` - Total sunshine duration
# MAGIC
# MAGIC ### 3. Array Explosion
# MAGIC Uses `F.explode()` to convert each daily record from a single row with 7 array elements into 7 individual rows (one per day).
# MAGIC
# MAGIC **Example transformation:**
# MAGIC ```
# MAGIC Before: 1 row per city with arrays of 7 elements
# MAGIC After:  7 rows per city (one row per forecast day)
# MAGIC ```
# MAGIC
# MAGIC ### 4. Data Extraction & Type Casting
# MAGIC Extracts fields from the exploded struct and casts to appropriate types:
# MAGIC
# MAGIC | Source Field | Target Column | Type | Description |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `d.time` | `forecast_date` | string | ISO 8601 forecast date |
# MAGIC | `d.temperature_2m_max` | `max_temp_c` | double | Maximum temperature (°C) |
# MAGIC | `d.temperature_2m_min` | `min_temp_c` | double | Minimum temperature (°C) |
# MAGIC | `d.apparent_temperature_max` | `feels_max_c` | double | Maximum feels-like temp (°C) |
# MAGIC | `d.apparent_temperature_min` | `feels_min_c` | double | Minimum feels-like temp (°C) |
# MAGIC | `d.precipitation_sum` | `rainfall_mm` | double | Total rainfall (mm) |
# MAGIC | `d.weather_code` | `weather_code` | int | WMO weather code |
# MAGIC | `d.sunrise` | `sunrise` | string | Sunrise timestamp |
# MAGIC | `d.sunset` | `sunset` | string | Sunset timestamp |
# MAGIC | `d.uv_index_max` | `uv_index` | double | Maximum UV index (0-15 scale) |
# MAGIC | `d.wind_speed_10m_max` | `wind_max_ms` | double | Maximum wind speed (m/s) |
# MAGIC | `d.wind_gusts_10m_max` | `gust_max_ms` | double | Maximum wind gust (m/s) |
# MAGIC | `d.sunshine_duration` | `sunshine_seconds` | double | Sunshine duration (seconds) |
# MAGIC
# MAGIC **Why Casting?** Bronze layer stores all values as strings in nested MAP/ARRAY structures. Explicit casting to numeric types (double/int) prevents downstream cast errors and ensures proper data types for analytics.
# MAGIC
# MAGIC ### 5. Derived Metrics
# MAGIC
# MAGIC #### Calendar Enrichment
# MAGIC - **day_name**: Day of week (e.g., "Monday", "Tuesday")  
# MAGIC   Uses `date_format("EEEE")`
# MAGIC - **is_weekend**: Boolean flag for Saturday/Sunday  
# MAGIC   Uses `dayofweek().isin([1,7])` (1=Sunday, 7=Saturday)
# MAGIC
# MAGIC #### Weather Conversions
# MAGIC - **sunshine_hours**: Converts seconds to hours  
# MAGIC   Formula: `sunshine_seconds ÷ 3600`, rounded to 2 decimals
# MAGIC - **wind_max_kmh**: Converts m/s to km/h  
# MAGIC   Formula: `wind_max_ms × 3.6`, rounded to 1 decimal
# MAGIC - **gust_max_kmh**: Converts gust speed to km/h  
# MAGIC   Formula: `gust_max_ms × 3.6`, rounded to 1 decimal
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
# MAGIC 1. **Forecast Date Not Null**: All records must have a forecast date
# MAGIC 2. **Temperature Logical Consistency**: Max temperature must be ≥ min temperature
# MAGIC 3. **Non-negative Rainfall**: Rainfall cannot be negative
# MAGIC 4. **UV Index Range**: Must be between 0-15 (standard UV index scale)
# MAGIC 5. **Non-negative Sunshine**: Sunshine hours cannot be negative
# MAGIC
# MAGIC If any assertion fails, the notebook stops execution and raises an error.
# MAGIC
# MAGIC ## 📊 Output Schema
# MAGIC
# MAGIC ### Silver Table: `weather_forcast.silver.daily_weather`
# MAGIC
# MAGIC **Write Mode:** Overwrite (replaces all data on each run)  
# MAGIC **Format:** Delta Lake
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | city | string | City name |
# MAGIC | country | string | Country name |
# MAGIC | forecast_date | string | ISO 8601 forecast date |
# MAGIC | max_temp_c | double | Maximum temperature (°C) |
# MAGIC | min_temp_c | double | Minimum temperature (°C) |
# MAGIC | feels_max_c | double | Maximum apparent temperature (°C) |
# MAGIC | feels_min_c | double | Minimum apparent temperature (°C) |
# MAGIC | rainfall_mm | double | Total precipitation (mm) |
# MAGIC | weather_code | int | WMO weather code |
# MAGIC | sunrise | string | Sunrise timestamp (ISO 8601) |
# MAGIC | sunset | string | Sunset timestamp (ISO 8601) |
# MAGIC | uv_index | double | Maximum UV index (0-15) |
# MAGIC | wind_max_ms | double | Maximum wind speed (m/s) |
# MAGIC | gust_max_ms | double | Maximum wind gust (m/s) |
# MAGIC | sunshine_seconds | double | Sunshine duration (seconds) |
# MAGIC | day_name | string | Day of week (full name) |
# MAGIC | is_weekend | boolean | Weekend indicator |
# MAGIC | sunshine_hours | double | Sunshine duration (hours) |
# MAGIC | wind_max_kmh | double | Maximum wind speed (km/h) |
# MAGIC | gust_max_kmh | double | Maximum wind gust (km/h) |
# MAGIC
# MAGIC **Data Volume:**
# MAGIC - 7 rows per city (7-day forecast)
# MAGIC - Total rows = 7 × number of cities in Bronze
# MAGIC
# MAGIC ## 🎯 Usage
# MAGIC
# MAGIC ### Standalone Execution
# MAGIC ```python
# MAGIC %run ./silver_daily_weather
# MAGIC ```
# MAGIC
# MAGIC ### As Part of Pipeline
# MAGIC This notebook is designed to run after the Bronze ingestion:
# MAGIC 1. **Bronze Notebook** → Fetches raw weather data
# MAGIC 2. **Current Weather Notebook** → Transforms current conditions
# MAGIC 3. **Hourly Weather Notebook** → Transforms hourly forecasts
# MAGIC 4. **This Notebook** → Transforms daily forecasts
# MAGIC 5. **Gold Notebook** → Creates business-level aggregates
# MAGIC
# MAGIC ### Downstream Consumption
# MAGIC Analytics queries can directly use `weather_forcast.silver.daily_weather`:
# MAGIC
# MAGIC ```sql
# MAGIC -- Find weekend forecasts with high UV
# MAGIC SELECT 
# MAGIC     city,
# MAGIC     forecast_date,
# MAGIC     day_name,
# MAGIC     max_temp_c,
# MAGIC     uv_index,
# MAGIC     sunshine_hours
# MAGIC FROM weather_forcast.silver.daily_weather
# MAGIC WHERE is_weekend = true
# MAGIC   AND uv_index > 7
# MAGIC ORDER BY uv_index DESC
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC -- Compare weekly temperature ranges by city
# MAGIC SELECT 
# MAGIC     city,
# MAGIC     AVG(max_temp_c) as avg_high,
# MAGIC     AVG(min_temp_c) as avg_low,
# MAGIC     MAX(max_temp_c) - MIN(min_temp_c) as temp_range,
# MAGIC     SUM(rainfall_mm) as total_rain
# MAGIC FROM weather_forcast.silver.daily_weather
# MAGIC GROUP BY city
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC -- Find days with extreme weather conditions
# MAGIC SELECT 
# MAGIC     city,
# MAGIC     forecast_date,
# MAGIC     max_temp_c,
# MAGIC     rainfall_mm,
# MAGIC     wind_max_kmh,
# MAGIC     gust_max_kmh
# MAGIC FROM weather_forcast.silver.daily_weather
# MAGIC WHERE max_temp_c > 35
# MAGIC    OR rainfall_mm > 50
# MAGIC    OR gust_max_kmh > 100
# MAGIC ORDER BY forecast_date
# MAGIC ```
# MAGIC
# MAGIC ## 🔍 Data Quality Notes
# MAGIC
# MAGIC ### Strengths
# MAGIC - Type-safe transformations with explicit casting
# MAGIC - Handles nested array structures efficiently
# MAGIC - Rich derived metrics (calendar + weather conversions)
# MAGIC - Automated data quality validations with logical consistency checks
# MAGIC - Business-friendly fields (day names, weekend flags)
# MAGIC
# MAGIC ### Key Design Decisions
# MAGIC
# MAGIC #### Why `arrays_zip()` + `explode()`?
# MAGIC This pattern efficiently unpacks multiple parallel arrays into individual rows:
# MAGIC - **Alternative 1**: 13 separate array explosions → expensive, complex joins
# MAGIC - **Alternative 2**: UDFs → harder to maintain, less optimized
# MAGIC - **Chosen approach**: Native Spark functions → optimal performance, single explosion
# MAGIC
# MAGIC #### Why Include Calendar Fields?
# MAGIC - **day_name** enables day-of-week analysis (weekday vs weekend patterns)
# MAGIC - **is_weekend** simplifies filtering for weekend forecasts
# MAGIC - Pre-computed calendar fields avoid repeated date functions in queries
# MAGIC
# MAGIC #### Why Three Wind Speed Representations?
# MAGIC - `wind_max_ms`: Raw API value (meters per second)
# MAGIC - `wind_max_kmh`: Common metric system unit
# MAGIC - `gust_max_ms`: Peak wind bursts (safety-critical for warnings)
# MAGIC
# MAGIC ### Limitations
# MAGIC - Overwrite mode means no historical tracking at Silver layer
# MAGIC - 7-day forecast coverage only
# MAGIC - Assumes all cities have complete 7-day arrays
# MAGIC - Sunrise/sunset remain as strings (could be cast to timestamp)
# MAGIC
# MAGIC ### Best Practices
# MAGIC - Run this notebook immediately after Bronze ingestion
# MAGIC - Monitor assertion failures in production (especially temperature consistency)
# MAGIC - Consider append mode if historical Silver data is needed
# MAGIC - For incremental processing, filter Bronze by `ingestion_timestamp`
# MAGIC - Extend validations for location-specific ranges (e.g., tropical vs polar temps)
# MAGIC
# MAGIC ## 🔧 Troubleshooting
# MAGIC
# MAGIC ### Common Issues
# MAGIC
# MAGIC **Assertion failures:**
# MAGIC - **max_temp < min_temp**: Check Bronze data quality; API may have swapped values
# MAGIC - **UV index out of range**: Rare but possible; may indicate API data issue
# MAGIC - **Negative sunshine hours**: Check sunshine_seconds field for nulls or invalid data
# MAGIC
# MAGIC **Type casting failures:**
# MAGIC - Inspect Bronze data for non-numeric values in numeric fields
# MAGIC - Add null handling if API returns missing values as empty strings
# MAGIC - Check for special values ("N/A", "null" as string)
# MAGIC
# MAGIC **Missing data after explode:**
# MAGIC - Verify Bronze table has `daily` field populated
# MAGIC - Check that `arrays_zip` includes all 13 required fields
# MAGIC - Some cities may have incomplete daily arrays (< 7 days)
# MAGIC
# MAGIC **Performance issues:**
# MAGIC - Daily data is much smaller than hourly (7 vs 168 rows per city)
# MAGIC - If processing many cities, consider partitioning by country
# MAGIC - Repartition before write if data is skewed
# MAGIC
# MAGIC ## 📅 Last Updated
# MAGIC 2026-08-22