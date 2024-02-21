# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-76ae333e-257d-4d9f-a001-8e61333d451c
# MAGIC %md
# MAGIC # Pseudonymized ETL
# MAGIC
# MAGIC This notebook allows you to programmatically:
# MAGIC
# MAGIC * Generate the DLT pipeline
# MAGIC * Trigger a pipeline run
# MAGIC * Explore the resultant DAG
# MAGIC * Land a new batch of data
# MAGIC
# MAGIC ## The Pipeline
# MAGIC The pipeline we are using in this lesson is located [here]($./Pipeline/ADE 3.1.2 - Pseudonymized ETL).

# COMMAND ----------

# DBTITLE 0,--i18n-0cd87fd0-f759-4335-bf81-86011b284039
# MAGIC %md
# MAGIC Run the following cell to configure your working environment for this course.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-03.2

# COMMAND ----------

# DBTITLE 0,--i18n-25030f74-feff-4434-b8dc-db72960feeb5
# MAGIC %md
# MAGIC
# MAGIC ## Generate DLT Pipeline
# MAGIC Run the cell below to auto-generate your DLT pipeline using the provided configuration values.
# MAGIC
# MAGIC Once the pipeline is ready, a link will be provided to navigate you to your auto-generated pipeline in the Pipeline UI.

# COMMAND ----------

DA.generate_pipeline()

# COMMAND ----------

# DBTITLE 0,--i18n-31ffb66b-1710-4b8c-ada8-cfbbd032080f
# MAGIC %md
# MAGIC ## Trigger Pipeline Run
# MAGIC
# MAGIC With a pipeline created, you will now run the pipeline. The initial run will take several minutes while a cluster is provisioned. Subsequent runs will be appreciably quicker.
# MAGIC
# MAGIC Explore the DAG - As the pipeline completes, the execution flow is graphed. With each triggered update, all newly arriving data will be processed through your pipeline. Metrics will always be reported for current run.

# COMMAND ----------

DA.start_pipeline()

# COMMAND ----------

# DBTITLE 0,--i18n-97a40e8d-3806-4ef4-8059-8ff0896b9c10
# MAGIC %md
# MAGIC ## Land New Data
# MAGIC
# MAGIC Run the cell below to land more data in the source directory, then manually trigger a pipeline update.
# MAGIC
# MAGIC As we continue through the course, you can return to this notebook and use the method provided below to land new data. Running this entire notebook again will delete the underlying data files for both the source data and your DLT Pipeline.

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

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
