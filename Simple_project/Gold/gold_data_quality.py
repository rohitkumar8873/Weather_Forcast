# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F

dim = spark.table("weather_forcast.gold.dim_location")
current = spark.table("weather_forcast.gold.fact_current_weather")
hourly = spark.table("weather_forcast.gold.fact_hourly_weather")
daily = spark.table("weather_forcast.gold.fact_daily_weather")
scd = spark.table("weather_forcast.gold.dim_weather_forecast")

dq = spark.createDataFrame([
("dim_location","Duplicate Locations",dim.count()-dim.select("location_id").distinct().count()),
("fact_current","Null Location",current.filter(F.col("location_id").isNull()).count()),
("fact_hourly","Negative Visibility",hourly.filter(F.col("visibility_m")<0).count()),
("fact_daily","Negative Rainfall",daily.filter(F.col("rainfall_mm")<0).count()),
("scd2","Multiple Current Records",
 scd.filter(F.col("is_current")==True)
 .groupBy("location_id","forecast_date")
 .count()
 .filter("count>1")
 .count())
],["table_name","validation","failed_records"])

dq.write.mode("overwrite").format("delta").saveAsTable("weather_forcast.gold.data_quality_report")

display(dq)

if dq.filter(F.col("failed_records")>0).count()>0:
    raise Exception("Gold Data Quality Failed")

# COMMAND ----------

display(scd)