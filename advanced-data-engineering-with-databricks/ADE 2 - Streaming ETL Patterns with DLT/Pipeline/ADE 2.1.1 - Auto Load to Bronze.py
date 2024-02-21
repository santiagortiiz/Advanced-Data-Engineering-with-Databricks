# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-27b497d6-df00-48cd-8fa7-88ee340345e1
# MAGIC %md
# MAGIC
# MAGIC # Auto Load Data to Bronze
# MAGIC
# MAGIC The chief architect has decided that rather than connecting directly to Kafka, a source system will send raw records as JSON files to cloud object storage. In this notebook, you'll build a multiplex table that will ingest these records with Auto Loader and store the entire history of this incremental feed. The initial table will store data from all of our topics and have the following schema. 
# MAGIC
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | key | BINARY |
# MAGIC | value | BINARY |
# MAGIC | topic | STRING |
# MAGIC | partition | LONG |
# MAGIC | offset | LONG
# MAGIC | timestamp | LONG |
# MAGIC | date | DATE |
# MAGIC | week_part | STRING |
# MAGIC
# MAGIC This single table will drive the majority of the data through the target architecture, feeding three interdependent data pipelines.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_bronze.png" width="60%" />
# MAGIC
# MAGIC **NOTE**: Details on additional configurations for connecting to Kafka are available <a href="https://docs.databricks.com/spark/latest/structured-streaming/kafka.html" target="_blank">here</a>.
# MAGIC
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - Describe a multiplex design
# MAGIC - Apply Auto Loader to incrementally process records

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

source = spark.conf.get("source")
lookup_db = spark.conf.get("lookup_db")

# COMMAND ----------

@dlt.table(
    table_properties={
        "pipelines.reset.allowed": "false"
    }
)
def date_lookup():
    return spark.read.table(f"{lookup_db}.date_lookup").select("date", "week_part")


@dlt.table(
    partition_cols=["topic", "week_part"],
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false"
    }
)
def bronze():
    return (
      spark.readStream
        .format("cloudFiles")
        .schema("key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG")
        .option("cloudFiles.format", "json")
        .load(f"{source}/daily")
        .join(
          F.broadcast(dlt.read("date_lookup")), 
          F.to_date((F.col("timestamp")/1000).cast("timestamp")) == F.col("date"), "left")
    )

# COMMAND ----------

@dlt.table
def distinct_topics():
    return dlt.read("bronze").select("topic").distinct()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
