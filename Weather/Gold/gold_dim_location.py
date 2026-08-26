# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS weather_forcast.gold")

dim_location = (
    spark.table("weather_forcast.silver.current_weather")
    .select("city","country","latitude","longitude")
    .dropDuplicates()
    .withColumn("location_id",F.monotonically_increasing_id())
)

dim_location.write.mode("overwrite").format("delta").saveAsTable("weather_forcast.gold.dim_location")
# display(spark.table("weather_forcast.silver.current_weather"))
# display(dim_location)