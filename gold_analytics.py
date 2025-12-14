# Databricks notebook source
from pyspark.sql.functions import (
    avg, max, min, when, col, to_date
)


# COMMAND ----------

silver_df = spark.table(
    "weather_catalog.processed.valid_readings"
)


# COMMAND ----------

silver_df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Layer Aggregation and Extreme Weather Detection

# COMMAND ----------

gold_df = (
    silver_df
    .groupBy("event_date", "location")
    .agg(
        avg("min_temp").alias("avg_min_temp"),
        avg("max_temp").alias("avg_max_temp"),
        avg("rainfall_mm").alias("avg_rainfall_mm"),
        avg("wind_speed_evening").alias("avg_wind_speed_evening"),
        avg("humidity_evening").alias("avg_humidity_evening"),
        max("max_temp").alias("max_temp"),
        min("min_temp").alias("min_temp")
    )
    # Extreme heat detection
    .withColumn(
        "extreme_heat_flag",
        when(col("max_temp") > 40, True).otherwise(False)
    )
    # Extreme cold detection (optional but powerful)
    .withColumn(
        "extreme_cold_flag",
        when(col("min_temp") < 5, True).otherwise(False)
    )
    # Temperature volatility (for UNIQUE insight)
    .withColumn(
        "temp_variation",
        col("max_temp") - col("min_temp")
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold layer data storage

# COMMAND ----------

gold_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("weather_catalog.analytics.weather_stats")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM weather_catalog.analytics.weather_stats
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT location, AVG(temp_variation) AS avg_temp_variation
# MAGIC FROM weather_catalog.analytics.weather_stats
# MAGIC GROUP BY location
# MAGIC ORDER BY avg_temp_variation DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT location, COUNT(*) AS extreme_heat_days
# MAGIC FROM weather_catalog.analytics.weather_stats
# MAGIC WHERE extreme_heat_flag = true
# MAGIC GROUP BY location
# MAGIC ORDER BY extreme_heat_days DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT location, AVG(avg_rainfall_mm) AS rainfall_during_heat
# MAGIC FROM weather_catalog.analytics.weather_stats
# MAGIC WHERE extreme_heat_flag = true
# MAGIC GROUP BY location
# MAGIC ORDER BY rainfall_during_heat ASC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT location, AVG(avg_max_temp) AS avg_max_temp
# MAGIC FROM weather_catalog.analytics.weather_stats
# MAGIC GROUP BY location;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT location, AVG(avg_min_temp) AS avg_min_temp
# MAGIC FROM weather_catalog.analytics.weather_stats
# MAGIC GROUP BY location;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT location, AVG(avg_wind_speed_evening) AS avg_wind_speed
# MAGIC FROM weather_catalog.analytics.weather_stats
# MAGIC GROUP BY location;
# MAGIC