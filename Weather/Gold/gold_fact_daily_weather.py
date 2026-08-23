# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F

location = spark.table("weather_forcast.gold.dim_location")
daily = spark.table("weather_forcast.silver.daily_weather")

fact = (
    daily.join(location,["city","country"])
    .select(
        "location_id",
        "city",
        "country",
        F.col("forecast_date").cast("date"),
        "day_name",
        "is_weekend",
        "max_temp_c",
        "min_temp_c",
        "rainfall_mm",
        "uv_index",
        "sunrise",
        "sunset",
        "sunshine_hours",
        "wind_max_kmh"
    )
)

fact.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("weather_forcast.gold.fact_daily_weather")

# COMMAND ----------

display(fact)