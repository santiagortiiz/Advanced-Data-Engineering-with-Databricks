# Databricks notebook source
# input_daily_raw.py
import dlt

def create_daily_raw_view(spark, source):
    @dlt.view(comment="Daily stream - Production data")
    def daily_raw_view():
      return (
        spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.inferColumnTypes", "true")
          .schema("key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG")
          .load(f"{source}/daily")
      )

# COMMAND ----------

# DBTITLE 0,--i18n-689a4988-0433-4d63-88de-abde928ccc0c
# MAGIC %md
# MAGIC ## Import Relative Library
# MAGIC
# MAGIC ```
# MAGIC from input_daily_raw import create_daily_raw_view
# MAGIC ```

# COMMAND ----------

source = spark.conf.get("source")
lookup_db = spark.conf.get("lookup_db")

create_daily_raw_view(spark, source)

# COMMAND ----------

import pyspark.sql.functions as F

@dlt.table(
  partition_cols=["topic", "week_part"],
  comment="Daily stream - Production data",
  table_properties={"quality": "bronze", "pipelines.reset.allowed": "false"}
)
@dlt.expect("valid_topic", "topic IS NOT NULL")
def daily_raw():
  return (
    dlt.read_stream("daily_raw_view")
      .withColumn("source_file", F.input_file_name())
      .withColumn("processed_timestamp", F.current_timestamp())
      .join(
        F.broadcast(spark.read.table(f"{lookup_db}.date_lookup").select("date", "week_part")),
        F.to_date("timestamp") == F.col("date"), "left"
      )
  )

# COMMAND ----------

# DBTITLE 0,--i18n-01235beb-1dce-4e86-aac4-5df46ca98c81
# MAGIC %md
# MAGIC
# MAGIC ## Parameterize Definition for Bronze Topic Tables
# MAGIC
# MAGIC Programmatically create tables for all distinct topics streaming from our raw daily Kafka stream.

# COMMAND ----------

def create_bronze_topic_table(table_name, topic, json_schema, select_columns):  
    @dlt.table(
        name=table_name,
        table_properties={"quality": "bronze"},
        comment=f"Parsed streaming data for bronze {topic} records"
    )
    def create_bronze_table():
        return (
            dlt.read_stream("daily_raw")
              .filter(f"topic = '{topic}'")
              .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
              .select("v.*")
              .select(*select_columns)
        )

# COMMAND ----------

# DBTITLE 0,--i18n-d2e52546-5c67-4562-80d7-52fc897e20d9
# MAGIC %md
# MAGIC
# MAGIC ## Generate Bronze Tables From Config

# COMMAND ----------

bronze_tables_config = {
    "bpm_bronze": {
        "topic": "bpm",
        "json_schema": "device_id LONG, time TIMESTAMP, heartrate DOUBLE",
        "select_columns": ["device_id", "time", "heartrate"]
    },
    "workouts_bronze": {
        "topic": "workout",
        "json_schema": "user_id INT, workout_id INT, timestamp FLOAT, action STRING, session_id INT",
        "select_columns": ["user_id", "workout_id", "timestamp", "action", "session_id"]
    },
    "users_cdc_bronze": {
        "topic": "user_info",
        "json_schema": "user_id LONG, update_type STRING, timestamp FLOAT, dob STRING, sex STRING, gender STRING, first_name STRING, last_name STRING, address STRUCT<street_address: STRING, city: STRING, state: STRING, zip: INT>",
        "select_columns": ["user_id", "timestamp", "dob", "sex", "gender", 
                            "first_name", "last_name", "address", "update_type"]
    }
} 

# COMMAND ----------

# Call function to generate bronze table for each topic

for table, config in bronze_tables_config.items():
    create_bronze_topic_table(table, config["topic"], config["json_schema"], config["select_columns"])

