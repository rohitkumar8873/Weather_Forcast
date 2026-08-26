# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F

current = spark.table("weather_forcast.gold.fact_current_weather")
daily = spark.table("weather_forcast.gold.fact_daily_weather")

kpi = current.groupBy("location_id").agg(
    F.first("temperature_c").alias("current_temp"),
    F.first("humidity").alias("humidity"),
    F.first("wind_kmh").alias("wind_speed")
)

forecast = daily.groupBy("location_id").agg(
    F.avg("max_temp_c").alias("avg_max_temp"),
    F.sum("rainfall_mm").alias("weekly_rain"),
    F.max("uv_index").alias("highest_uv")
)

dashboard = kpi.join(forecast,"location_id")

dashboard.write.mode("overwrite").format("delta").saveAsTable("weather_forcast.gold.dashboard_kpis")

# display(dashboard)