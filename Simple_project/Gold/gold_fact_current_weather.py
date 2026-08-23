# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F

location = spark.table("weather_forcast.gold.dim_location")
current = spark.table("weather_forcast.silver.current_weather")

fact = (
    current.join(location,["city","country"])
    .select(
        "location_id",
        "city",
        "country",
        "observation_time",
        "temperature_c",
        "temperature_f",
        "feels_like_c",
        "humidity",
        "wind_kmh",
        "weather_description",
        "temperature_category"
    )
)

fact.write.mode("overwrite").format("delta").saveAsTable("weather_forcast.gold.fact_current_weather")

# COMMAND ----------

display(fact)