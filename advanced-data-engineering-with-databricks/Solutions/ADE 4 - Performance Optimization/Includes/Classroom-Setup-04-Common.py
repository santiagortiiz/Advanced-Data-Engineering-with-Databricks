# Databricks notebook source
# MAGIC %run ../../Includes/_common

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

    # from pyspark.sql.functions import *

    # numFiles = 125
    # numRowsPerFile = 10000000

    # (spark
    # .range(0,numRowsPerFile * numFiles, numPartitions = numFiles)
    # #  .repartition(numFiles)
    # .selectExpr('*', 'RAND() as value', 'id % 100 as device_type')
    # .write
    # .mode('overwrite')
    # .saveAsTable('iot_data')
    # )

    return DA

