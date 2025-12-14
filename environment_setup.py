# Databricks notebook source
dbutils.fs.ls("s3://weather-stream-pavan/DataSet/")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS weather_catalog;
# MAGIC USE CATALOG weather_catalog;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS raw;
# MAGIC CREATE SCHEMA IF NOT EXISTS processed;
# MAGIC CREATE SCHEMA IF NOT EXISTS analytics;
# MAGIC CREATE SCHEMA IF NOT EXISTS logs;
# MAGIC