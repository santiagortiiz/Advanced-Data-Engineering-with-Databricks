# Databricks notebook source
# MAGIC %run ./Classroom-Setup-05-Common

# COMMAND ----------

PROD_LESSON = "prod"
DA = init_DA(PROD_LESSON)

# PROD STREAMING SOURCE
DA.daily_stream = StreamFactory(
    source_dir=DA.paths.datasets, 
    target_dir=DA.paths.stream_source,
    load_batch=load_daily_batch,
    max_batch=16
)
DA.daily_stream.load()

notebooks=[
    "bronze/prod/ingest"
]

# PROD PIPELINE
DA.configure_pipeline(configuration={"source": DA.paths.stream_source, "lookup_db": DA.lookup_db}, notebooks=notebooks)
DA.conclude_setup()

