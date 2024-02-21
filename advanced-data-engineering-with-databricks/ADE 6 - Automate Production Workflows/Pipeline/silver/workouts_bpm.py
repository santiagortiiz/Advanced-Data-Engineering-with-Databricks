# Databricks notebook source
# DBTITLE 0,--i18n-7eb161cc-c6cf-41ce-991a-7b2418a11352
# MAGIC %md
# MAGIC
# MAGIC ## Workouts BPM Silver Updates

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# COMMAND ----------

lookup_db = spark.conf.get("lookup_db")

def get_rules(topic):
    df = spark.read.table(f"{lookup_db}.rules").filter(F.col("topic") == topic)
    rules = {}
    for row in df.collect(): 
        rules[row["name"]] = row["condition"]
    return rules

# COMMAND ----------

@dlt.table(table_properties={"quality": "silver"})
@dlt.expect_all_or_drop(get_rules("bpm"))
def bpm_silver():
    return (
        dlt.read_stream("valid_bpm")
          .select("device_id", "time", "heartrate")
          .withColumn("bpm_check", F.when(F.col("heartrate") <= 0, "Negative BPM").otherwise("OK"))
          .withWatermark("time", "30 seconds")
          .dropDuplicates(["device_id", "time"])
    )


@dlt.table(table_properties={"quality": "silver"})
@dlt.expect_all_or_drop(get_rules("workouts"))
def workouts_silver():
    return (
        dlt.read_stream("valid_workouts")
          .select("user_id", "workout_id", 
                  F.col("timestamp").cast("timestamp").alias("time"), 
                  "action", "session_id")
          .withWatermark("time", "30 seconds")
          .dropDuplicates(["user_id", "time"])
    )


@dlt.table
def workouts_completed():
    return spark.sql(f"""
      SELECT a.user_id, a.workout_id, a.session_id, a.start_time start_time, b.end_time end_time, a.in_progress AND (b.in_progress IS NULL) in_progress
      FROM (
        SELECT user_id, workout_id, session_id, time start_time, null end_time, true in_progress
        FROM LIVE.workouts_silver
        WHERE action = "start") a
      LEFT JOIN (
        SELECT user_id, workout_id, session_id, null start_time, time end_time, false in_progress
        FROM LIVE.workouts_silver
        WHERE action = "stop") b
      ON a.user_id = b.user_id AND a.session_id = b.session_id
    """)

lookup_db = spark.conf.get("lookup_db")


@dlt.table
def workout_bpm():
    return spark.sql(f"""
      SELECT d.user_id, d.workout_id, d.session_id, time, heartrate
      FROM STREAM(LIVE.bpm_silver) c
      INNER JOIN (
        SELECT a.user_id, b.device_id, workout_id, session_id, start_time, end_time
        FROM LIVE.workouts_completed a
        INNER JOIN LIVE.user_lookup b
        ON a.user_id = b.user_id) d
      ON c.device_id = d.device_id AND time BETWEEN start_time AND end_time
      WHERE c.bpm_check = 'OK'
    """)
    

