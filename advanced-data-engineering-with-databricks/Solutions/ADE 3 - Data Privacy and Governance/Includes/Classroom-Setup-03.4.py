# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

# MAGIC %run ../../Includes/_stream_factory

# COMMAND ----------

def init_DA(name, reset=True):
    """
    Initializes DBAcademyHelper with paths to data sources and storage locations.
    :param name: lesson name used to create isolated environments
    :param reset: if true, runs clean up before initialization
    """

    lesson_config = LessonConfig(name=name,
                                create_schema=True,
                                create_catalog=False,
                                requires_uc=False,
                                installing_datasets=True,
                                enable_streaming_support=True,
                                enable_ml_support=False)

    DA = DBAcademyHelper(course_config, lesson_config)
    DA.reset_lesson()
    DA.init()

    DA.paths.stream_source = f"{DA.paths.working_dir}/stream_source"
    DA.paths.silver_source = f"{DA.paths.datasets}/pii/silver"
    DA.paths.cdc_stream = f"{DA.paths.stream_source}/cdc"

    return DA

# COMMAND ----------

LESSON = "cdf_demo"
DA = init_DA(LESSON)

DA.cdc_stream = StreamFactory(
    source_dir=DA.paths.datasets, 
    target_dir=DA.paths.stream_source,
    load_batch=load_cdc_batch,
    max_batch=3
)

# DA.cdc_stream.load()
DA.conclude_setup()

None


