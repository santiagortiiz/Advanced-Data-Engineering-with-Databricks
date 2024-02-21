# Databricks notebook source
# MAGIC %run ./Classroom-Setup-05-Common

# COMMAND ----------

prod_lesson = "prod"
dev_lesson = "dev"
test_lesson = "test"

DA = init_DA(dev_lesson, reset=False) # No lesson environment reset
DA.paths.pipeline_event_logs = f"{DA.paths.storage_location}/system/events"
DA.conclude_setup()

