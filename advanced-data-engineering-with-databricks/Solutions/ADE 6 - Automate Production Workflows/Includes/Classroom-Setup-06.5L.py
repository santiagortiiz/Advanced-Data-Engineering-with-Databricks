# Databricks notebook source
# MAGIC %sh rm -f $(which databricks); curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# COMMAND ----------

# MAGIC %run ./Classroom-Setup-06-Common

# COMMAND ----------

LESSON = "deploy-workload"
DA = init_DA(LESSON, pipeline=True)

(db_token,db_instance) = DA.load_credentials()

try:
    notebooks=[
        "bronze/prod/ingest",
        "silver/quarantine",
        "silver/workouts_bpm",
        "silver/users"    
    ]
    DA.configure_pipeline(configuration={"source": DA.paths.stream_source, "lookup_db": DA.lookup_db}, notebooks=notebooks)
    import os
    os.environ['DATABRICKS_PIPELINE_ID'] = DA.generate_pipeline()
except:
    pass

DA.daily_stream = StreamFactory(
    source_dir=DA.paths.datasets, 
    target_dir=DA.paths.stream_source,
    load_batch=load_daily_batch,
    max_batch=16
)
DA.daily_stream.load()

None

