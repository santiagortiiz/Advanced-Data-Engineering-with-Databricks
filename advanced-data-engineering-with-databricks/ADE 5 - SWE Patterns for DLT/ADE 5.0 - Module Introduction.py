# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-3bbb7147-a23b-4721-b575-b1e14d2b425d
# MAGIC %md
# MAGIC
# MAGIC ## SWE Patterns with DLT
# MAGIC This module is part of the Data Engineer Learning Path by Databricks Academy.
# MAGIC
# MAGIC #### Lessons
# MAGIC Lecture: SWE Practices with DLT <br>
# MAGIC [ADE 5.1 - Import Sample Data]($./ADE 5.1 - Import Sample Data) <br>
# MAGIC [ADE 5.2 - Demo - Modularize Code]($./ADE 5.2 - Modularize Code) <br>
# MAGIC [ADE 5.3 - Demo - Develop Pipelines]($./ADE 5.3 - Develop Pipelines) <br>
# MAGIC [ADE 5.4 - Demo - Unit Test Pipelines]($./ADE 5.4 - Unit Test Pipelines) <br>
# MAGIC [ADE 5.5 - Demo - Monitor Data Quality]($./ADE 5.5 - Monitor Data Quality) <br>
# MAGIC [ADE 5.6L - Develop and Test Pipelines Lab]($./ADE 5.6L - Develop and Test Pipelines Lab) <br>
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
