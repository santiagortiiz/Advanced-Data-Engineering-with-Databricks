# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

from pyspark.sql.types import StructType

lesson_config = LessonConfig(name="structured_streaming",
                             create_schema=True,
                             create_catalog=False,
                             requires_uc=False,
                             installing_datasets=True,
                             enable_streaming_support=True,
                             enable_ml_support=False)

DA = DBAcademyHelper(course_config, lesson_config)
DA.reset_lesson()
DA.init()

DA.paths.sales = f"{DA.paths.datasets}/ecommerce/delta/sales_hist"
DA.paths.users = f"{DA.paths.datasets}/ecommerce/delta/users_hist"
DA.paths.events = f"{DA.paths.datasets}/ecommerce/delta/events_hist"
DA.paths.products = f"{DA.paths.datasets}/delta/item_lookup"

DA.conclude_setup()

