# Databricks notebook source
# MAGIC %run ./Classroom-Setup-02-Common

# COMMAND ----------

notebooks = [
    "ADE 2.1.1 - Auto Load to Bronze",
    "ADE 2.2.1 - Stream from Multiplex Bronze",
    "ADE 2.3.1 - Data Quality Enforcement",
    "ADE 2.4.1L - Streaming ETL Lab",    
    # "../Includes/Lab-Solution/ADE 2.4.1L - Streaming ETL Lab",
    "ADE 2.5.1 - Data Modeling - SCD Type 2",
    "ADE 2.6.1 - Streaming Joins"
]

DA.configure_pipeline(configuration={"source": DA.paths.stream_source, "lookup_db": DA.lookup_db}, notebooks=notebooks)
DA.conclude_setup()

None


