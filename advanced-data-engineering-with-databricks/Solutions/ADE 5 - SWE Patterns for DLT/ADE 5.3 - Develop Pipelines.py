# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-af0c25bc-64b1-46a2-a458-450e27b3028c
# MAGIC %md
# MAGIC # Develop Pipelines
# MAGIC
# MAGIC ##### Objectives
# MAGIC - Configure an isolated environment for pipeline development using target schema locations
# MAGIC - Manage code for ingest and transform logic in segmented libraries for multiple deployment environments
# MAGIC - Use pipeline parameters to control data sources and volume in development environments
# MAGIC - Prepare sample datasets for development environments
# MAGIC
# MAGIC Generate and trigger an update of a pipeline consisting of the following notebooks:
# MAGIC
# MAGIC | Pipeline |
# MAGIC |---|
# MAGIC | [Bronze / Dev / Ingest Subset]($./Pipeline/bronze/prod/ingest) |
# MAGIC | Silver / Quarantine |
# MAGIC | Silver / Users |
# MAGIC | Silver / Workouts BPM |
# MAGIC
# MAGIC
# MAGIC Start by running the following setup cell to configure your working environment.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-05.3

# COMMAND ----------

# DBTITLE 0,--i18n-f0730ae4-4819-440f-b4fd-43ffe1e278a8
# MAGIC %md
# MAGIC
# MAGIC ## Isolate Pipeline Environments
# MAGIC
# MAGIC Isolate environments for dev, test, and prod pipelines by configuring different target schemas and storage locations for each environment.  
# MAGIC For example, following settings could be used to configure the same ETL pipeline for different environments.
# MAGIC
# MAGIC ##### Production
# MAGIC - Name: `ETL Pipeline - Prod`
# MAGIC - Target: `prod_schema`
# MAGIC - Storage: `/path/to/prod-storage`
# MAGIC
# MAGIC ##### Development
# MAGIC - Name: `ETL Pipeline - Dev`
# MAGIC - Target: `dev_schema`
# MAGIC - Storage: `/path/to/dev-storage`
# MAGIC
# MAGIC ##### Test
# MAGIC - Name: `ETL Pipeline - Test`
# MAGIC - Target: `test_schema`
# MAGIC - Storage: `/path/to/test-storage`
# MAGIC
# MAGIC The development pipeline targets a development database and uses a storage location for development.  
# MAGIC The test pipeline targets a test database and uses a storage location for testing.  
# MAGIC These pipelines would be maintained in a separate environment from the production pipeline.

# COMMAND ----------

# DBTITLE 0,--i18n-585ccbc1-634e-454c-9324-5da62737d197
# MAGIC %md
# MAGIC ### Segment Ingest and Transform Libraries
# MAGIC
# MAGIC We can modularize our pipeline to easily reuse the same pipeline transformation code on different data sources. 
# MAGIC
# MAGIC The settings below can be used to configure libraries for pipelines that use the same transformation libraries, while using a different ingestion library to replace `<ingest-library>` below.
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC   "libraries": [
# MAGIC       "notebook": {"path": <ingest-library> },
# MAGIC       "notebook": {"path": ".../transform_silver.py"},
# MAGIC       "notebook": {"path": ".../transform_gold.py"}
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Say we have three separate libraries that ingest data from different sources into bronze tables for a pipeline.
# MAGIC - `ingest-prod/ingest.py` loads from a kafka stream for production
# MAGIC - `ingest-dev/ingest-subset.py` loads a subset of production data for development
# MAGIC - `ingest-dev/ingest-sample.py` loads sample datasets created for unit testing
# MAGIC
# MAGIC Each of the ingest libraries above define the same tables used as input data in the shared transformation library `transform_silver.py`. For example, all three libraries define a `workouts_bronze` table that is used as input for the `workouts_silver` table in the transformation library.

# COMMAND ----------

# DBTITLE 0,--i18n-9d5dacb4-b48e-4207-a8a5-89d186cd0afb
# MAGIC %md
# MAGIC
# MAGIC ## Create Pipeline
# MAGIC Run the cell below to auto-generate your DLT pipeline using the provided configuration values. Once the pipeline is ready, a link will be provided to navigate you to your auto-generated pipeline in the Pipeline UI.

# COMMAND ----------

DA.generate_pipeline()

# COMMAND ----------

# DBTITLE 0,--i18n-b6e4e508-7553-47bc-8e4d-f4216c7aeeb2
# MAGIC %md
# MAGIC
# MAGIC ## Trigger a Pipeline Update
# MAGIC Use the method provided below to trigger a pipeline update.

# COMMAND ----------

DA.start_pipeline(DA.pipeline_id)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
