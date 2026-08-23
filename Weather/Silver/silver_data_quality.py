# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F

current_df = spark.table("weather_forcast.silver.current_weather")
hourly_df = spark.table("weather_forcast.silver.hourly_weather")
daily_df = spark.table("weather_forcast.silver.daily_weather")

dq_summary = spark.createDataFrame([
    ("current_weather","Null City",current_df.filter(F.col("city").isNull()).count()),
    ("current_weather","Null Temperature",current_df.filter(F.col("temperature_c").isNull()).count()),
    ("current_weather","Invalid Humidity",current_df.filter((F.col("humidity")<0)|(F.col("humidity")>100)).count()),
    ("hourly_weather","Invalid Rain Probability",hourly_df.filter((F.col("rain_probability")<0)|(F.col("rain_probability")>100)).count()),
    ("hourly_weather","Negative Visibility",hourly_df.filter(F.col("visibility_m")<0).count()),
    ("daily_weather","Max Temp Less Than Min Temp",daily_df.filter(F.col("max_temp_c")<F.col("min_temp_c")).count()),
    ("daily_weather","Negative Rainfall",daily_df.filter(F.col("rainfall_mm")<0).count()),
    ("daily_weather","Invalid UV Index",daily_df.filter((F.col("uv_index")<0)|(F.col("uv_index")>15)).count())
],["table_name","validation","failed_records"])

dq_summary.write.mode("overwrite").format("delta").saveAsTable("weather_forcast.silver.data_quality_report")

display(dq_summary)

# Fail the pipeline if any validation fails
failed = dq_summary.filter(F.col("failed_records") > 0).count()

if failed > 0:
    raise Exception(f"Data Quality Failed: {failed} validation(s) failed.")