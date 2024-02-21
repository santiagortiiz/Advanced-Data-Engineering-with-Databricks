# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-d966d14f-da4a-4fcd-9556-c640ecc02d31
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Quality Enforcement
# MAGIC
# MAGIC One of the main motivations for using Delta Lake to store data is that you can provide guarantees on the quality of your data. While schema enforcement is automatic, additional quality checks can be helpful to ensure that only data that meets your expectations makes it into your Lakehouse.
# MAGIC
# MAGIC In this lesson, we'll perform data quality checks on heart rate data from the BPM bronze table configured in the previous lesson. We'll quarantine invalid records, perform streaming deduplication, and apply data quality tags beforer writing to our heart rate silver table.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - Apply **`dropDuplicates`** to streaming data
# MAGIC - Use watermarking to manage state information
# MAGIC - Describe and implement a quarantine table
# MAGIC - Apply logic to add data quality tags to Delta tables

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 0,--i18n-e85768c9-21d9-4297-a14b-d7ab1f67c84e
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Streaming Deduplication
# MAGIC While Spark Structured Streaming provides exactly-once processing guarantees, many source systems will introduce duplicate records, which must be removed in order for joins and updates to produce logically correct results in downstream queries.

# COMMAND ----------

# DBTITLE 0,--i18n-07f7c05c-b16e-44c3-9973-2ad614a05266
# MAGIC %md
# MAGIC ## Flagging
# MAGIC To avoid multiple writes and managing multiple tables, you may choose to implement a flagging system to warn about violations while avoiding job failures. Flagging is a low touch solution with little overhead. These flags can easily be leveraged by filters in downstream queries to isolate bad data. **`case`** / **`when`** logic makes this easy.

# COMMAND ----------

rules = {
  "valid_heartrate": "heartrate IS NOT NULL",
  "valid_device_id": "device_id IS NOT NULL",
  "valid_device_id_range": "device_id > 110000"
}

@dlt.table(
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop(rules)
def bpm_silver():
    return (
        dlt.read_stream("bpm_bronze")
          .select("*", F.when(F.col("heartrate") <= 0, "Negative BPM").otherwise("OK").alias("bpm_check"))
          .withWatermark("time", "30 seconds")
          .dropDuplicates(["device_id", "time"])
    )

# COMMAND ----------

# DBTITLE 0,--i18n-50dbf4c7-e3cb-4619-abed-abc5877ef7f4
# MAGIC %md
# MAGIC ## Quarantining
# MAGIC
# MAGIC The idea of quarantining is that bad records will be written to a separate location. This allows good data to processed efficiently, while additional logic and/or manual review of erroneous records can be defined and executed away from the main pipeline. Assuming that records can be successfully salvaged, they can be easily backfilled into the silver table they were deferred from.
# MAGIC
# MAGIC For simplicity, we won't check for duplicate records as we insert data into the quarantine table.

# COMMAND ----------

quarantine_rules = {}
quarantine_rules["invalid_record"] = f"NOT({' AND '.join(rules.values())})"

@dlt.table
@dlt.expect_all_or_drop(quarantine_rules)
def bpm_quarantine():
    return (
        dlt.read_stream("bpm_bronze")
        )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
