# Databricks notebook source
# MAGIC %run ./Classroom-Setup-03-Common

# COMMAND ----------

import pyspark.sql.functions as F

notebooks = [
    "ADE 3.1.1 - PII Lookup Table"
]
DA.configure_pipeline(configuration={"source": DA.paths.stream_source, "lookup_db": DA.lookup_db}, notebooks=notebooks)
DA.user_reg_stream.load()
DA.conclude_setup()

None


