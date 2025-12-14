# Databricks notebook source
# MAGIC %md
# MAGIC ### JSON Parsing and Schema Setup

# COMMAND ----------

from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Weather Data Schema Definition

# COMMAND ----------

weather_schema = StructType([
    StructField("Date", StringType()),
    StructField("Location", StringType()),
    StructField("MinTemp", DoubleType()),
    StructField("MaxTemp", DoubleType()),
    StructField("Rainfall", DoubleType()),
    StructField("WindSpeed9am", DoubleType()),
    StructField("WindSpeed3pm", DoubleType()),
    StructField("Humidity9am", DoubleType()),
    StructField("Humidity3pm", DoubleType()),
    StructField("RainToday", StringType()),
    StructField("RainTomorrow", StringType())
])


# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze JSON Parsing and Column Extraction

# COMMAND ----------

bronze_df = spark.table("weather_catalog.raw.weather_readings")

parsed_df = (
    bronze_df
    .select(
        from_json(col("json_event"), weather_schema).alias("data"),
        col("ingest_time")
    )
    .select("data.*", "ingest_time")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Column Renaming

# COMMAND ----------

silver_df = (
    parsed_df
    .withColumnRenamed("Date", "event_date")
    .withColumnRenamed("Location", "location")
    .withColumnRenamed("MinTemp", "min_temp")
    .withColumnRenamed("MaxTemp", "max_temp")
    .withColumnRenamed("Rainfall", "rainfall_mm")
    .withColumnRenamed("WindSpeed9am", "wind_speed_morning")
    .withColumnRenamed("WindSpeed3pm", "wind_speed_evening")
    .withColumnRenamed("Humidity9am", "humidity_morning")
    .withColumnRenamed("Humidity3pm", "humidity_evening")
    .withColumnRenamed("RainToday", "rain_today")
    .withColumnRenamed("RainTomorrow", "rain_tomorrow")
    .withColumn("event_date", to_date(col("event_date")))
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality Validation

# COMMAND ----------

silver_clean_df = (
    silver_df
    .filter("location IS NOT NULL")
    .filter("min_temp IS NOT NULL AND max_temp IS NOT NULL")
    .filter("min_temp > -50 AND max_temp < 60")
    .filter("rainfall_mm >= 0 OR rainfall_mm IS NULL")
    .filter("humidity_morning BETWEEN 0 AND 100 OR humidity_morning IS NULL")
    .filter("humidity_evening BETWEEN 0 AND 100 OR humidity_evening IS NULL")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer Data Storage

# COMMAND ----------

silver_clean_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("weather_catalog.processed.valid_readings")


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE weather_catalog.processed.valid_readings;
# MAGIC