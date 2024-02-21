# Databricks notebook source
# MAGIC %run ./Classroom-Setup-05-Common

# COMMAND ----------

DA = init_DA("lab")

notebooks = [
    "lab/bronze/dev/ingest_test",
    "lab/silver/quarantine",
    "lab/silver/users",
    "lab/silver/workouts_bpm",
    "lab/tests/users_test",
    "lab/tests/workouts_test"
]

DA.configure_pipeline(configuration={"source": DA.paths.test_data, "lookup_db": DA.lookup_db}, notebooks=notebooks)
DA.conclude_setup()

