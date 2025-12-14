# Databricks notebook source
# MAGIC %md
# MAGIC ### Define source path (S3)

# COMMAND ----------

source_path = "s3://weather-stream-pavan/DataSet/Weather_Aus.csv"

bronze_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(source_path)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer – Raw JSON Event Creation

# COMMAND ----------

from pyspark.sql.functions import to_json, struct, current_timestamp

bronze_final_df = (
    bronze_df
    .withColumn("json_event", to_json(struct(*bronze_df.columns)))
    .withColumn("ingest_time", current_timestamp())
    .select("json_event", "ingest_time")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer – Raw Data Storage

# COMMAND ----------

bronze_final_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("weather_catalog.raw.weather_readings")
