# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-99b2413f-821d-4c2a-8d1c-cfbe6f2fd3a9
# MAGIC %md
# MAGIC ## Important: <br>
# MAGIC ### This lab needs to be completed before moving on to ADE 2.5 and ADE 2.6.

# COMMAND ----------

# DBTITLE 0,--i18n-4bbd8fb0-d5a7-4d7d-8aef-a4b6e3620796
# MAGIC %md
# MAGIC # Streaming ETL Lab
# MAGIC
# MAGIC This notebook allows you to programmatically generate and trigger an update of a DLT pipeline that consists of the following notebooks:
# MAGIC
# MAGIC |DLT Pipeline|
# MAGIC |---|
# MAGIC |Auto Load to Bronze|
# MAGIC |Stream from Multiplex Bronze|
# MAGIC |Quality Enforcement|
# MAGIC |[Streaming ETL]($./Pipeline/ADE 2.4.1L - Streaming ETL Lab)|
# MAGIC
# MAGIC As we continue through the course, you can return to this notebook and use the provided methods to:
# MAGIC - Land a new batch of data
# MAGIC - Trigger a pipeline update
# MAGIC - Process all remaining data
# MAGIC
# MAGIC **NOTE:** Re-running the entire notebook will delete the underlying data files for both the source data and your DLT Pipeline.

# COMMAND ----------

# DBTITLE 0,--i18n-1b604708-5002-4514-9507-98c3ca9dbf1b
# MAGIC %md
# MAGIC ## Run Setup
# MAGIC Run the following cell to reset and configure your working environment for this course.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-02.4L

# COMMAND ----------

# DBTITLE 0,--i18n-cd55c12c-a984-4b5c-9b70-4ddb0cad2960
# MAGIC %md
# MAGIC
# MAGIC ## Generate DLT Pipeline
# MAGIC Run the cell below to auto-generate your DLT pipeline using the provided configuration values.
# MAGIC
# MAGIC Once the pipeline is ready, a link will be provided to navigate you to your auto-generated pipeline in the Pipeline UI.

# COMMAND ----------

DA.generate_pipeline()

# COMMAND ----------

# DBTITLE 0,--i18n-eb330805-7a4a-4bcd-8432-861c88d4a95c
# MAGIC %md
# MAGIC ## Trigger Pipeline Run
# MAGIC
# MAGIC With a pipeline created, you will now run the pipeline. The initial run will take several minutes while a cluster is provisioned. Subsequent runs will be appreciably quicker.
# MAGIC
# MAGIC Explore the DAG - As the pipeline completes, the execution flow is graphed. With each triggered update, all newly arriving data will be processed through your pipeline. Metrics will always be reported for current run.
# MAGIC
# MAGIC **IMPORTANT:** You'll need to complete the lab in the DLT notebook [ADE 2.4.1L - Streaming ETL]($./Pipeline/ADE 2.4.1L - Streaming ETL Lab) for this pipeline to run successfully.

# COMMAND ----------

# TODO: Make sure to complete the lab in the DLT notebook linked above (ADE 2.4.1L) before running this pipeline
DA.start_pipeline()

# COMMAND ----------

# DBTITLE 0,--i18n-ebd886d3-e719-4781-9192-e166cb88f21f
# MAGIC %md
# MAGIC ## Land New Data
# MAGIC
# MAGIC Run the cell below to land more data in the source directory, then manually trigger another pipeline update using the UI or the cell above.

# COMMAND ----------

DA.daily_stream.load()

# COMMAND ----------

# DBTITLE 0,--i18n-63e2735a-d777-4aca-bcc9-ee7c3e449ad8
# MAGIC %md
# MAGIC ## Process All Remaining Data
# MAGIC To continuously load all remaining batches of data to the source directory, call the same load method above with the **`continuous`** parameter set to **`True`**.
# MAGIC
# MAGIC Trigger another update to process the remaining data.

# COMMAND ----------

DA.daily_stream.load(continuous=True)  # Load all remaining batches of data
DA.start_pipeline()  # Trigger another pipeline update

# COMMAND ----------

# DBTITLE 0,--i18n-19e2e09a-0127-4346-ba46-f74e22e56a3a
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
