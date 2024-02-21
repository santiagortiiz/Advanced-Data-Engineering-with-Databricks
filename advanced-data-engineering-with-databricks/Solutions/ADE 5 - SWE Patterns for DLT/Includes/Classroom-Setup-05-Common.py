# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

# MAGIC %run ../../Includes/_stream_factory

# COMMAND ----------

# MAGIC %run ../../Includes/_pipeline_config

# COMMAND ----------

import pandas as pd

def init_DA(name, reset=True):
    """
    Initializes DBAcademyHelper with paths to data sources and storage locations.
    :param name: lesson name used to create isolated pipeline environments
    :param reset: if true, runs clean up before initialization
    """

    lesson_config = LessonConfig(name=name, 
                    create_schema=True,
                    create_catalog = False,
                    requires_uc = False,
                    installing_datasets = True,
                    enable_streaming_support = False,
                    enable_ml_support = False)
    DA = DBAcademyHelper(course_config, lesson_config)
    if reset: DA.reset_lesson()
    DA.init()

    DA.paths.stream_source = f"{DA.paths.working_dir}/stream_source"
    DA.paths.test_data = f"{DA.paths.datasets}/sample-datasets"

    DA.paths.storage_location = f"{DA.paths.working_dir}/storage_location"
    DA.pipeline_name = f"{DA.unique_name(sep='-')}: ETL Pipeline - {name.upper()}"

    DA.lookup_db = f"{DA.unique_name(sep='_')}_lookup"

    if reset:  # create lookup tables
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DA.lookup_db}")
        DA.clone_source_table(f"{DA.lookup_db}.date_lookup", source_name="date-lookup")
        DA.clone_source_table(f"{DA.lookup_db}.user_lookup", source_name="user-lookup")

        df = spark.createDataFrame(pd.DataFrame([
            ("device_id_not_null", "device_id IS NOT NULL", "validity", "bpm"),
            ("device_id_valid_range", "device_id > 110000", "validity", "bpm"),
            ("heartrate_not_null", "heartrate IS NOT NULL", "validity", "bpm"),
            ("user_id_not_null", "user_id IS NOT NULL", "validity", "workout"),
            ("workout_id_not_null", "workout_id IS NOT NULL", "validity", "workout"),
            ("action_not_null","action IS NOT NULL","validity","workout"),
            ("user_id_not_null","user_id IS NOT NULL","validity","user_info"),
            ("update_type_not_null","update_type IS NOT NULL","validity","user_info")
        ], columns=["name", "condition", "tag", "topic"]))
        df.write.mode("overwrite").saveAsTable(f"{DA.lookup_db}.rules")
        

    return DA

