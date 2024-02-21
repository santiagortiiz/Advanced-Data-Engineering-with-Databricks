# Databricks notebook source
# MAGIC %run ./Classroom-Setup-02-Common

# COMMAND ----------

notebooks = [
    "ADE 2.1.1 - Auto Load to Bronze",
    "ADE 2.2.1 - Stream from Multiplex Bronze",
    "ADE 2.3.1 - Data Quality Enforcement"
]

DA.configure_pipeline(configuration={"source": DA.paths.stream_source, "lookup_db": DA.lookup_db}, notebooks=notebooks)
DA.conclude_setup()

None


