# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp

gold_df = spark.table(
    "weather_catalog.analytics.weather_stats"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Extreme Weather Anomaly Identification

# COMMAND ----------

anomaly_df = (
    gold_df
    .filter(
        (col("extreme_heat_flag") == True) |
        (col("extreme_cold_flag") == True)
    )
    .withColumn("log_time", current_timestamp())
    .withColumn(
        "anomaly_type",
        col("extreme_heat_flag")
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Anomaly Classification

# COMMAND ----------

from pyspark.sql.functions import when

anomaly_df = anomaly_df.withColumn(
    "anomaly_description",
    when(col("extreme_heat_flag") == True, "EXTREME_HEAT")
    .when(col("extreme_cold_flag") == True, "EXTREME_COLD")
)


# COMMAND ----------

log_df = anomaly_df.select(
    "event_date",
    "location",
    "max_temp",
    "min_temp",
    "anomaly_description",
    "log_time"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Log data storage

# COMMAND ----------

log_df.write.format("delta") \
    .mode("append") \
    .saveAsTable("weather_catalog.logs.anomaly_log")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM weather_catalog.logs.anomaly_log
# MAGIC ORDER BY log_time DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Slack Alert Notification

# COMMAND ----------

import requests

SLACK_WEBHOOK_URL = ""

alerts = log_df.collect()

for row in alerts:
    message = (
        f"Weather Alert\n"
        f"Location: {row['location']}\n"
        f"Date: {row['event_date']}\n"
        f"Type: {row['anomaly_description']}\n"
        f"Max Temp: {row['max_temp']}°C\n"
        f"Min Temp: {row['min_temp']}°C"
    )

    requests.post(
        SLACK_WEBHOOK_URL,
        json={"text": message}
    )
