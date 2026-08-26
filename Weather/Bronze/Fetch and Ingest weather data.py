# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import requests
import json
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("city", "delhi")

city = dbutils.widgets.get("city").strip().lower()

print(f"Processing city: {city}")

# COMMAND ----------

# Geocoding API
geo = requests.get(
    "https://geocoding-api.open-meteo.com/v1/search",
    params={
        "name": city,
        "count": 1,
        "language": "en",
        "format": "json"
    }
).json()

if "results" not in geo:
    raise Exception(f"City '{city}' not found.")

location = geo["results"][0]

lat = location["latitude"]
lon = location["longitude"]
country = location["country"]

# COMMAND ----------

# Weather API
weather = requests.get(
    "https://api.open-meteo.com/v1/forecast",
    params={
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,apparent_temperature,relative_humidity_2m,wind_speed_10m,wind_gusts_10m,weather_code,is_day,cloud_cover,surface_pressure",
        "hourly": "temperature_2m,relative_humidity_2m,precipitation_probability,wind_speed_10m,wind_gusts_10m,cloud_cover,visibility",
        "daily": "temperature_2m_max,temperature_2m_min,apparent_temperature_max,apparent_temperature_min,precipitation_sum,weather_code,sunrise,sunset,uv_index_max,wind_speed_10m_max,wind_gusts_10m_max,sunshine_duration",
        "forecast_days": 7,
        "timezone": "auto"
    }
).json()
# print(weather)

# COMMAND ----------

# Add metadata
weather["city"] = city
weather["country"] = country
weather["latitude"] = lat
weather["longitude"] = lon

print(weather["city"])

# COMMAND ----------

# Create DataFrame with unique key
df = spark.createDataFrame([weather]) \
          .withColumn("ingestion_timestamp", F.current_timestamp()) \
          .withColumn("record_key", F.xxhash64(F.col("city"),F.col("generationtime_ms")))
print(df)

# COMMAND ----------

# Create Bronze schema
spark.sql("CREATE SCHEMA IF NOT EXISTS weather_forcast.bronze")

# COMMAND ----------

table_name = "weather_forcast.bronze.weather_raw"

# Make city column NOT NULL and add PRIMARY KEY constraint to optimize MERGE performance (idempotent)
try:
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN city SET NOT NULL")
except Exception:
    pass  # Column may already be NOT NULL

try:
    spark.sql(f"ALTER TABLE {table_name} ADD CONSTRAINT weather_raw_pk PRIMARY KEY(city)")
except Exception:
    pass  # Constraint may already exist

# Create a temporary view for the new city data
df.createOrReplaceTempView("new_weather")

# Update existing city or insert new city
# city is always lowercase (see widget setup in cell 2), so direct equality
# avoids unnecessary LOWER() calls and enables better join optimization
spark.sql(f"""
MERGE INTO {table_name} AS target
USING new_weather AS source
ON target.city = source.city

WHEN MATCHED THEN
    UPDATE SET *

WHEN NOT MATCHED THEN
    INSERT *
""")

print(f"✓ Bronze data updated for city: {city}")

# COMMAND ----------

# Save raw JSON - dynamic mode with schema evolution
df.write \
  .mode("append") \
  .format("delta") \
  .option("mergeSchema", "true") \
  .saveAsTable("weather_forcast.bronze.weather_raw")

# display(df)

# COMMAND ----------

# DBTITLE 1,Notebook Documentation
# MAGIC %md
# MAGIC # Weather Data Ingestion - Bronze Layer
# MAGIC
# MAGIC ## 📋 Overview
# MAGIC This notebook fetches real-time weather data from the Open-Meteo API and ingests it into the Bronze layer of the weather forecast data pipeline. It retrieves current conditions, hourly forecasts, and daily forecasts for a specified city.
# MAGIC
# MAGIC ## 🔧 Parameters
# MAGIC - **city** (String): The city name for weather data retrieval
# MAGIC   - Default: `Kolkata`
# MAGIC   - Example: `New York`, `London`, `Tokyo`
# MAGIC
# MAGIC ## 📊 Data Sources
# MAGIC 1. **Geocoding API**: `https://geocoding-api.open-meteo.com/v1/search`
# MAGIC    - Converts city name to coordinates (latitude/longitude)
# MAGIC    - Returns country information
# MAGIC
# MAGIC 2. **Weather API**: `https://api.open-meteo.com/v1/forecast`
# MAGIC    - Provides current weather conditions
# MAGIC    - Returns 7-day hourly and daily forecasts
# MAGIC    - Includes temperature, humidity, precipitation, wind speed, weather codes
# MAGIC
# MAGIC ## 🔄 Process Flow
# MAGIC
# MAGIC ### 1. Geocoding
# MAGIC - Accepts city name from parameter widget
# MAGIC - Queries geocoding API to get coordinates
# MAGIC - Extracts latitude, longitude, and country
# MAGIC
# MAGIC ### 2. Weather Data Fetch
# MAGIC - Uses coordinates to query weather forecast API
# MAGIC - Retrieves:
# MAGIC   - **Current**: temperature, feels-like, humidity, wind speed, weather code
# MAGIC   - **Hourly**: temperature, humidity, precipitation probability, wind speed (7 days)
# MAGIC   - **Daily**: min/max temperature, precipitation sum, weather code, sunrise/sunset (7 days)
# MAGIC
# MAGIC ### 3. Data Enrichment
# MAGIC - Adds metadata: city, country, latitude, longitude
# MAGIC - Adds ingestion timestamp
# MAGIC
# MAGIC ### 4. Bronze Layer Storage
# MAGIC - Creates DataFrame from weather JSON
# MAGIC - Saves to Delta table: `weather_forcast.bronze.weather_raw`
# MAGIC - Mode: Append with schema evolution enabled
# MAGIC
# MAGIC ### 5. Task Integration
# MAGIC - Passes weather JSON to next notebook task using `dbutils.jobs.taskValues.set()`
# MAGIC - Key: `weather_json`
# MAGIC
# MAGIC ## 📤 Output
# MAGIC
# MAGIC ### Delta Table
# MAGIC - **Location**: `weather_forcast.bronze.weather_raw`
# MAGIC - **Format**: Delta
# MAGIC - **Schema**: Dynamic (schema evolution enabled)
# MAGIC - **Write Mode**: Append
# MAGIC
# MAGIC ### Key Columns
# MAGIC - `city`: City name
# MAGIC - `country`: Country name
# MAGIC - `latitude`, `longitude`: Geographic coordinates
# MAGIC - `timezone`: Timezone identifier
# MAGIC - `elevation`: Elevation in meters
# MAGIC - `current`: Nested current weather data
# MAGIC - `hourly`: Nested hourly forecast data (168 hours)
# MAGIC - `daily`: Nested daily forecast data (7 days)
# MAGIC - `ingestion_timestamp`: When data was ingested
# MAGIC
# MAGIC ## 🎯 Usage
# MAGIC
# MAGIC ### Standalone Execution
# MAGIC ```python
# MAGIC # Run with default city (Kolkata)
# MAGIC %run ./"Fetch and Ingest weather data"
# MAGIC
# MAGIC # Or set city parameter
# MAGIC dbutils.widgets.text("city", "New York", "Enter City")
# MAGIC ```
# MAGIC
# MAGIC ### As Part of Job Workflow
# MAGIC This notebook is designed to be the first task in a multi-task job:
# MAGIC 1. Fetches and stores raw weather data
# MAGIC 2. Passes processed JSON to downstream tasks
# MAGIC 3. Downstream tasks can access via `dbutils.jobs.taskValues.get()`
# MAGIC
# MAGIC ## 🔍 Data Quality Notes
# MAGIC - API calls may fail if city is not found
# MAGIC - Weather data is fetched in real-time (not cached)
# MAGIC - Schema evolution handles API changes automatically
# MAGIC - Each run appends new data (no deduplication at Bronze layer)
# MAGIC
# MAGIC ## 📅 Last Updated
# MAGIC 2026-08-22