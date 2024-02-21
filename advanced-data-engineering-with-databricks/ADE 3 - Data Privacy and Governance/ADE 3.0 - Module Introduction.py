# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-b171fd33-1632-4632-b849-9d569d938634
# MAGIC %md
# MAGIC ## Data Privacy and Governnance
# MAGIC This module is part of the Data Engineer Learning Path by Databricks Academy.
# MAGIC
# MAGIC #### Lessons
# MAGIC Lecture: Store Data Securely (slides available in .pdf) <br>
# MAGIC [ADE 3.1 - Demo - PII Lookup Table]($./ADE 3.1 - PII Lookup Table) <br>
# MAGIC [ADE 3.2 - Demo - Pseudonymized ETL]($./ADE 3.2 - Pseudonymized ETL) <br>
# MAGIC [ADE 3.3 - Demo - Deidentified PII Access]($./ADE 3.3 - Deidentified PII Access) <br>
# MAGIC Lecture: Streaming Data and CDF (slides available in .pdf) <br>
# MAGIC Lecture: Deleting Data in the Lakehouse <br>
# MAGIC [ADE 3.4 - Demo - Processing Records from CDF]($./ADE 3.4 - Processing Records from CDF) <br>
# MAGIC [ADE 3.5L - Propagating Changes with CDF Lab]($./ADE 3.5L - Propagating Changes with CDF Lab) <br>
# MAGIC [ADE 3.6 - Demo - Propagating Deletes with CDF]($./ADE 3.6 - Propagating Deletes with CDF) <br>
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
