# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F

location = spark.table("weather_forcast.gold.dim_location")
hourly = spark.table("weather_forcast.silver.hourly_weather")

fact = (
    hourly.join(location,["city","country"])
    .select(
        "location_id",
        "forecast_time",
        "city",
        "country",
        "temperature_c",
        "humidity",
        "rain_probability",
        "wind_kmh",
        "cloud_cover",
        "visibility_m"
    )
)

fact.write.mode("overwrite").format("delta").saveAsTable("weather_forcast.gold.fact_hourly_weather")

# COMMAND ----------

display(fact)