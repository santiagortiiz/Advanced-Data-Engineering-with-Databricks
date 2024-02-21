# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-cfdec218-97c0-43db-936b-cb88248c9547
# MAGIC %md
# MAGIC ### Important: <br>
# MAGIC ### This lab needs to be completed before moving on to ADE 2.5 and ADE 2.6. <br>
# MAGIC ### If you cannot figure out the answers, you can fill them in using the answers in the solutions folder

# COMMAND ----------

# DBTITLE 0,--i18n-f5343ce2-b784-40eb-b6c2-9b3833d01bd5
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Streaming ETL Lab
# MAGIC ## Process Workouts
# MAGIC
# MAGIC In this lab, you will configure a query to consume and parse raw data from a single topic as it lands in a multiplex bronze table. You'll also validate and quarantine these records before loading them into a silver table.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - Describe how filters are applied to streaming jobs
# MAGIC - Use built-in functions to flatten nested JSON data
# MAGIC - Parse and save binary-encoded strings to native types
# MAGIC - Describe and implement a quarantine table
# MAGIC
# MAGIC ## Hint
# MAGIC
# MAGIC Each step of this lab has you define one additional table in the pipeline. Work on this lab one step at a time. Once you think you have a step implemented, run the pipeline to verify whether the table has the correct results. If not, update the code in this notebook. Then in the DLT Pipeline interface, select only the table you're working on for a refresh, and perform a **full refresh** on that table.

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 0,--i18n-92186341-b8e6-40c0-a477-5b5ac81978a3
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Step 1: Stream Workouts from Multiplex Bronze
# MAGIC
# MAGIC Stream records for the **`workout`** topic from the multiplex bronze table to create a **`workouts_bronze`** table.
# MAGIC 1. Start a stream against the **`bronze`** table
# MAGIC 1. Filter all records by **`topic = 'workout'`**
# MAGIC 1. Parse and flatten JSON fields

# COMMAND ----------

# TODO

workouts_schema = "user_id INT, workout_id INT, timestamp FLOAT, action STRING, session_id INT"


# COMMAND ----------

# DBTITLE 0,--i18n-21230e07-0299-409c-b232-853ce9666acb
# MAGIC %md
# MAGIC
# MAGIC ## Step 2: Promote Workouts to Silver
# MAGIC
# MAGIC Process workouts bronze data into a **`workouts_silver`** table with the following schema.
# MAGIC
# MAGIC | field | type |
# MAGIC | --- | --- |
# MAGIC | user_id | INT | 
# MAGIC | workout_id | INT | 
# MAGIC | time | TIMESTAMP |
# MAGIC | action | STRING |
# MAGIC | session_id | INT | 
# MAGIC
# MAGIC Validate and promote records from **`workouts_bronze`** to silver by implementing a **`workouts_silver`** table.
# MAGIC 1. Check that **`user_id`** and **`workout_id`** fields are not null
# MAGIC 1. Cast **`timestamp`** to timestamp field named **`time`**
# MAGIC 1. Deduplicate on **`user_id`** and **`time`**

# COMMAND ----------

# TODO

# COMMAND ----------

# DBTITLE 0,--i18n-b50b8eb4-d0a1-4025-841d-fbf0ca3905c5
# MAGIC %md
# MAGIC
# MAGIC ## Step 3: Quarantine Invalid Records
# MAGIC
# MAGIC Implement a **`workouts_quarantine`** table for invalid records from **`workouts_bronze`**.

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
