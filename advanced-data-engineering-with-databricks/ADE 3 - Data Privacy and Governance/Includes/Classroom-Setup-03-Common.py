# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

# MAGIC %run ../../Includes/_stream_factory

# COMMAND ----------

# MAGIC %run ../../Includes/_pipeline_config

# COMMAND ----------

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
    DA.paths.storage_location = f"{DA.paths.working_dir}/storage_location"
    DA.pipeline_name = f"{DA.unique_name(sep='-')}: Pipeline - {name}"

    DA.lookup_db = f"{DA.unique_name(sep='_')}_lookup"

    if reset:  # create lookup tables
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DA.lookup_db}")
        DA.clone_source_table(f"{DA.lookup_db}.date_lookup", source_name="date-lookup")
        DA.clone_source_table(f"{DA.lookup_db}.user_lookup", source_name="user-lookup")

    return DA

# COMMAND ----------

LESSON = "pii"
DA = init_DA(LESSON)

DA.user_reg_stream = StreamFactory(
    source_dir=DA.paths.datasets, 
    target_dir=DA.paths.stream_source,
    load_batch=load_user_reg_batch,
    max_batch=16
)

DA.daily_stream = StreamFactory(
    source_dir=DA.paths.datasets, 
    target_dir=DA.paths.stream_source,
    load_batch=load_daily_batch,
    max_batch=16
)

None

