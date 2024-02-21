# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-6962d888-222c-4bde-8345-e644ed01ca8e
# MAGIC %md
# MAGIC ## Streaming ETL Patterns with DLT
# MAGIC This module is part of the Data Engineer Learning Path by Databricks Academy.
# MAGIC
# MAGIC #### Lessons
# MAGIC Lecture: Data Ingestions Patterns (slides available in .pdf) <br>
# MAGIC [ADE 2.1 - Demo: Auto Load to Bronze]($./ADE 2.1 - Auto Load to Bronze) <br>
# MAGIC [ADE 2.2 - Demo: Stream from Multiplex Bronze]($./ADE 2.2 - Stream from Multiplex Bronze) <br>
# MAGIC Lecture: Data Quality Enforcement Patterns (slides available in .pdf) <br>
# MAGIC [ADE 2.3 - Demo: Data Quality Enforcement]($./ADE 2.3 - Data Quality Enforcement) <br>
# MAGIC [ADE 2.4L - Streaming ETL Lab]($./ADE 2.4L - Streaming ETL Lab) <br>
# MAGIC Lecture: Data Modeling (slides available in .pdf) <br>
# MAGIC [ADE 2.5 - Demo: Data Modeling: SCD Type 2]($./ADE 2.5 - Data Modeling - SCD Type 2) <br>
# MAGIC Lecture: Streaming Joins and Statefulness (slides available in .pdf) <br>
# MAGIC [ADE 2.6 - Demo: Streaming Joins]($./ADE 2.6 - Streaming Joins) <br>
# MAGIC
# MAGIC
# MAGIC #### Prerequisites
# MAGIC * Ability to perform basic code development tasks using the Databricks Data Engineering & Data Science workspace (create clusters, run code in notebooks, use basic notebook operations, import repos from git, etc)
# MAGIC * Intermediate programming experience with PySpark
# MAGIC   * Extract data from a variety of file formats and data sources
# MAGIC   * Apply a number of common transformations to clean data
# MAGIC   * Reshape and manipulate complex data using advanced built-in functions
# MAGIC * Intermediate programming experience with Delta Lake (create tables, perform complete and incremental updates, compact files, restore previous versions etc.)
# MAGIC * Beginner experience configuring and scheduling data pipelines using the Delta Live Tables (DLT) UI
# MAGIC * Beginner experience defining Delta Live Tables pipelines using PySpark
# MAGIC   * Ingest and process data using Auto Loader and PySpark syntax
# MAGIC   * Process Change Data Capture feeds with APPLY CHANGES INTO syntax
# MAGIC   * Review pipeline event logs and results to troubleshoot DLT syntax
# MAGIC
# MAGIC
# MAGIC #### Technical Considerations
# MAGIC * This course runs on DBR 13.3.
# MAGIC * This course cannot be delivered on Databricks Community Edition.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
