# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-254143ad-58aa-41f3-b964-9b69b56b86fc
# MAGIC %md
# MAGIC # Stream from Multiplex Bronze
# MAGIC ## Bronze to Silver
# MAGIC
# MAGIC This notebook allows you to programmatically generate and trigger an update of a DLT pipeline that consists of the following notebooks:
# MAGIC
# MAGIC |DLT Pipeline|
# MAGIC |---|
# MAGIC |Auto Load to Bronze|
# MAGIC |[Stream from Multiplex Bronze]($./Pipeline/ADE 2.2.1 - Stream from Multiplex Bronze)|
# MAGIC
# MAGIC As we continue through the course, you can return to this notebook and use the provided methods to:
# MAGIC - Land a new batch of data
# MAGIC - Trigger a pipeline update
# MAGIC - Process all remaining data
# MAGIC
# MAGIC **NOTE:** Re-running the entire notebook will delete the underlying data files for both the source data and your DLT Pipeline.

# COMMAND ----------

# DBTITLE 0,--i18n-7795bee9-637c-489a-bc86-694d4ee3646a
# MAGIC %md
# MAGIC ## Run Setup
# MAGIC Run the following cell to reset and configure your working environment for this course.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-02.2

# COMMAND ----------

# DBTITLE 0,--i18n-00e9c894-277d-44fe-ad7d-3d731f9649e4
# MAGIC %md
# MAGIC
# MAGIC ## Generate DLT Pipeline
# MAGIC Run the cell below to auto-generate your DLT pipeline using the provided configuration values.
# MAGIC
# MAGIC Once the pipeline is ready, a link will be provided to navigate you to your auto-generated pipeline in the Pipeline UI.

# COMMAND ----------

DA.generate_pipeline()

# COMMAND ----------

# DBTITLE 0,--i18n-57a2fd03-9902-442a-befd-37b63e37097e
# MAGIC %md
# MAGIC ## Trigger Pipeline Run
# MAGIC
# MAGIC With a pipeline created, you will now run the pipeline. The initial run will take several minutes while a cluster is provisioned. Subsequent runs will be appreciably quicker.
# MAGIC
# MAGIC Explore the DAG - As the pipeline completes, the execution flow is graphed. With each triggered update, all newly arriving data will be processed through your pipeline. Metrics will always be reported for current run.

# COMMAND ----------

DA.start_pipeline()

# COMMAND ----------

# DBTITLE 0,--i18n-54757b26-7ff7-45c2-92de-2499701afedb
# MAGIC %md
# MAGIC ## Land New Data
# MAGIC
# MAGIC Run the cell below to land more data in the source directory, then manually trigger another pipeline update using the UI or the cell above.

# COMMAND ----------

DA.daily_stream.load()

# COMMAND ----------

# DBTITLE 0,--i18n-fd65d861-efd2-410b-b686-5bdaec15c96e
# MAGIC %md
# MAGIC ## Process All Remaining Data
# MAGIC To continuously load all remaining batches of data to the source directory, call the same load method above with the **`continuous`** parameter set to **`True`**.
# MAGIC
# MAGIC Trigger another update to process the remaining data.

# COMMAND ----------

DA.daily_stream.load(continuous=True)  # Load all remaining batches of data
DA.start_pipeline()  # Trigger another pipeline update

# COMMAND ----------

# DBTITLE 0,--i18n-da44b9cc-c9e4-452c-8bb1-09ce412ab10f
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
