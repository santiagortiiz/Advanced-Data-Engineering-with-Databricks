# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-83de31a2-e904-4a13-ba02-8b42b76fc085
# MAGIC %md
# MAGIC # Streaming Joins
# MAGIC ## Silver to Gold
# MAGIC
# MAGIC This notebook allows you to programmatically generate and trigger an update of a DLT pipeline that consists of the following notebooks:
# MAGIC
# MAGIC |DLT Pipeline|
# MAGIC |---|
# MAGIC |Auto Load to Bronze|
# MAGIC |Stream from Multiplex Bronze|
# MAGIC |Data Quality Enforcement|
# MAGIC |Streaming ETL Lab|
# MAGIC |Data Modeling: SCD Type 2|
# MAGIC |[Streaming Joins]($./Pipeline/ADE 2.6.1 - Streaming Joins)|
# MAGIC
# MAGIC As we continue through the course, you can return to this notebook and use the provided methods to:
# MAGIC - Land a new batch of data
# MAGIC - Trigger a pipeline update
# MAGIC - Process all remaining data
# MAGIC
# MAGIC **NOTE:** Re-running the entire notebook will delete the underlying data files for both the source data and your DLT Pipeline.
# MAGIC
# MAGIC #Important Note:# 
# MAGIC If you have not completed ADE 2.4L, this pipeline will fail. If you would like to run this pipeline without completing the lab, you will need to change the path to the lab notebook to the completed lab notebook found in the "Solutions" folder.

# COMMAND ----------

# DBTITLE 0,--i18n-7ddaac44-1bc5-420a-b46e-510de681dbac
# MAGIC %md
# MAGIC ## Run Setup
# MAGIC Run the following cell to reset and configure your working environment for this course.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-02.6

# COMMAND ----------

# DBTITLE 0,--i18n-42f34324-d5d6-41af-9b2c-e52a4b2cb760
# MAGIC %md
# MAGIC
# MAGIC ## Generate DLT Pipeline
# MAGIC Run the cell below to auto-generate your DLT pipeline using the provided configuration values.
# MAGIC
# MAGIC Once the pipeline is ready, a link will be provided to navigate you to your auto-generated pipeline in the Pipeline UI.

# COMMAND ----------

DA.generate_pipeline()

# COMMAND ----------

# DBTITLE 0,--i18n-d6e20971-5393-4b4e-8088-046ad526db98
# MAGIC %md
# MAGIC ## Trigger Pipeline Run
# MAGIC
# MAGIC With a pipeline created, you will now run the pipeline. The initial run will take several minutes while a cluster is provisioned. Subsequent runs will be appreciably quicker.
# MAGIC
# MAGIC Explore the DAG - As the pipeline completes, the execution flow is graphed. With each triggered update, all newly arriving data will be processed through your pipeline. Metrics will always be reported for current run.

# COMMAND ----------

DA.start_pipeline()

# COMMAND ----------

# DBTITLE 0,--i18n-b7d2485a-30ad-43d1-9e5b-2ef1ab48d93f
# MAGIC %md
# MAGIC ## Land New Data
# MAGIC
# MAGIC Run the cell below to land more data in the source directory, then manually trigger another pipeline update using the UI or the cell above.

# COMMAND ----------

DA.daily_stream.load()

# COMMAND ----------

# DBTITLE 0,--i18n-40c2984b-11c3-4521-9005-bca1f547a9be
# MAGIC %md
# MAGIC ## Process All Remaining Data
# MAGIC To continuously load all remaining batches of data to the source directory, call the same load method above with the **`continuous`** parameter set to **`True`**.
# MAGIC
# MAGIC Trigger another update to process the remaining data.

# COMMAND ----------

DA.daily_stream.load(continuous=True)  # Load all remaining batches of data
DA.start_pipeline()  # Trigger another pipeline update

# COMMAND ----------

# DBTITLE 0,--i18n-f711c5c4-3908-47d6-b114-840373d04fba
# MAGIC %md
# MAGIC ## Run Cleanup
# MAGIC Run the following cell to reset your working environment.

# COMMAND ----------

# DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
