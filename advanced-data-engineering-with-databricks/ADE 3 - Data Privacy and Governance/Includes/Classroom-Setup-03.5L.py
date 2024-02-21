# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

lesson_name = "cdf_lab"

lesson_config = LessonConfig(name=lesson_name,
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

DA.clone_source_table("bronze", source_name="bronze")
DA.clone_source_table("user_lookup", source_name="user-lookup")
DA.clone_source_table("users", source_name="prod_db/users_silver")
DA.clone_source_table("user_bins", source_name="prod_db/user_bins")

DA.conclude_setup()

