# Databricks notebook source
# MAGIC %run ./Classroom-Setup-05-Common

# COMMAND ----------

DA = init_DA("test")

notebooks = [
    "bronze/dev/ingest_test",
    "silver/quarantine",
    "silver/users",
    "silver/workouts_bpm",
    "tests/users_test"
]

DA.configure_pipeline(configuration={"source": DA.paths.test_data, "lookup_db": DA.lookup_db}, notebooks=notebooks)
DA.conclude_setup()


