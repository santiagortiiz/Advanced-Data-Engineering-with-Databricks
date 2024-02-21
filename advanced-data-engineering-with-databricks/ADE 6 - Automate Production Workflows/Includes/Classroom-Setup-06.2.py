# Databricks notebook source
# MAGIC %run ./Classroom-Setup-06-Common

# COMMAND ----------

LESSON = "using_cli"
DA = init_DA(LESSON, reset=False, pipeline=True)

(db_token,db_instance) = DA.load_credentials()

None

