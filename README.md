Real-Time Weather Data Processing Pipeline

📌 Project Overview

The Real-Time Weather Data Processing Pipeline is a data engineering project built using Python, PySpark, Databricks, and Delta Lake. It processes weather data through multiple layers, performs data quality checks, generates analytical summaries, and identifies extreme weather conditions.

The project follows a Bronze–Silver–Gold architecture to organize raw data, cleaned records, and analytics-ready datasets.

🎯 Objectives

- Ingest weather data from CSV files.
- Convert raw weather records into structured JSON events.
- Parse and transform weather data using PySpark.
- Validate data quality and filter invalid records.
- Generate aggregated weather statistics.
- Detect extreme heat and cold conditions.
- Store weather anomaly logs and prepare alert notifications.

🛠️ Technology Stack

- Programming Language: Python
- Data Processing: PySpark
- Platform: Databricks
- Storage Format: Delta Lake
- Data Querying: Spark SQL
- Source Data: Weather CSV dataset
- Notifications: Slack Webhook integration logic

🏗️ Pipeline Architecture

Weather CSV Dataset
        |
        v
Bronze Layer
Raw ingestion and JSON event creation
        |
        v
Silver Layer
JSON parsing, schema enforcement,
column transformation and data validation
        |
        v
Gold Layer
Weather aggregations and analytics
        |
        v
Weather Anomaly Detection
Extreme heat and cold identification
        |
        v
Anomaly Logs and Alert Notifications

⚙️ Key Features

1. Bronze Layer — Data Ingestion

- Reads weather data from a CSV source.
- Converts records into JSON event structures.
- Adds ingestion timestamps.
- Stores raw records in a Delta table.

2. Silver Layer — Data Processing

- Defines a structured schema for weather records.
- Parses JSON events into individual columns.
- Standardizes column names and converts date fields.
- Validates temperature, rainfall, and humidity values.
- Filters records with missing or invalid data.
- Stores cleaned records in Delta Lake.

3. Gold Layer — Analytics

- Groups weather data by event date and location.
- Calculates average minimum and maximum temperatures.
- Calculates average rainfall, wind speed, and humidity.
- Identifies extreme heat and extreme cold conditions.
- Calculates temperature variation for further analysis.
- Stores analytics-ready weather statistics in a Delta table.

4. Logging and Alerts

- Identifies records flagged as extreme weather events.
- Classifies anomalies as extreme heat or extreme cold.
- Creates timestamped anomaly logs.
- Includes Slack webhook notification logic for alert messages.

Note: Actual alert delivery depends on completing and configuring the Slack webhook.

📂 Repository Structure

Real-Time-Weather-Data-Process-Pipeline/
│
├── bronze_ingestion.py
├── silver_processing.py
├── gold_analytics.py
├── logging_and_alerts.py
├── environment_setup.py
└── README.md

🚀 Setup and Execution

1. Clone the repository:
   
   git clone https://github.com/PavanKumar4786/Real-Time-Weather-Data-Process-Pipeline.git

2. Open the project in a Databricks environment with the required Spark and Delta Lake support.

3. Configure access to the weather CSV dataset and update the source path in the ingestion code.

4. Run the pipeline components in order:
   
   1. "environment_setup.py"
   2. "bronze_ingestion.py"
   3. "silver_processing.py"
   4. "gold_analytics.py"
   5. "logging_and_alerts.py"

5. Verify the Delta tables, aggregated results, and anomaly logs in your Databricks environment.

📊 Expected Outcomes

- Structured and validated weather records.
- Analytics-ready weather statistics by location and date.
- Identification of potential extreme temperature events.
- Persistent Delta tables for processed data and analytics.
- Prepared anomaly logging and notification workflow.

Actual results depend on the source dataset and successful execution of the pipeline.

👨‍💻 Author

Pavan Kumar Reddy Yarrabolu

GitHub: "PavanKumar4786" (https://github.com/PavanKumar4786)

Project Repository: "Real-Time Weather Data Processing Pipeline" (https://github.com/PavanKumar4786/Real-Time-Weather-Data-Process-Pipeline)

---

This project demonstrates practical skills in data ingestion, ETL processing, data quality validation, distributed data processing, analytical aggregation, and layered data architecture.
