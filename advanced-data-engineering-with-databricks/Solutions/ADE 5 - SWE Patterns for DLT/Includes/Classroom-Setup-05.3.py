# Databricks notebook source
# MAGIC %run ./Classroom-Setup-05-Common

# COMMAND ----------

DEV_LESSON = "dev"
DA = init_DA(DEV_LESSON)

DA.prod_clone_db = f"{DA.schema_name_prefix}_prod_clone"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DA.prod_clone_db}")
DA.clone_source_table(f"{DA.prod_clone_db}.bpm_bronze", source_name="prod_db/bpm_bronze")
DA.clone_source_table(f"{DA.prod_clone_db}.workouts_bronze", source_name="prod_db/workouts_bronze")
DA.clone_source_table(f"{DA.prod_clone_db}.users_cdc_bronze", source_name="prod_db/users_cdc_bronze")

notebooks = [
    "bronze/dev/ingest_subset",
    "silver/quarantine",
    "silver/workouts_bpm",
    "silver/users"      
]

# DEV PIPELINE
DA.configure_pipeline(configuration={"production_db": DA.prod_clone_db, "lookup_db": DA.lookup_db}, notebooks=notebooks)
DA.conclude_setup()

