# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-1aebea64-f6a7-4725-b79a-0188244af3d7
# MAGIC %md
# MAGIC ## Incremental Processing with Spark Structured Streaming
# MAGIC This module is part of the Data Engineer Learning Path by Databricks Academy. 
# MAGIC
# MAGIC #### Lessons
# MAGIC Lecture: Streaming Data Concepts (slides available in .pdf)<br>
# MAGIC Lecture: Introduction to Structured Streaming (slides available in .pdf) <br>
# MAGIC [ADE 1.1 - Reading from a Streaming Query]($./ADE 1.1 - Reading from a Streaming Query) <br>
# MAGIC [ADE 1.2L - Streaming Query Lab]($./ADE 1.2L - Streaming Query Lab) <br>
# MAGIC Lecture: Aggregations, Time Windows, Watermarks (slides available in .pdf) <br>
# MAGIC [ADE 1.3L - Stream Aggregations Lab]($./ADE 1.3L - Stream Aggregations Lab) <br>
# MAGIC Lecture: Delta Live Tables Review (slides available in .pdf) <br>
# MAGIC Lecture: Auto Loader (slides available in .pdf) <br>
# MAGIC
# MAGIC #### Prerequisites
# MAGIC * Ability to perform basic code development tasks using the Databricks Data Engineering & Data Science workspace (create clusters, run code in notebooks, use basic notebook operations, import repos from git, etc)
# MAGIC * Intermediate programming experience with PySpark
# MAGIC   * Extract data from a variety of file formats and data sources
# MAGIC   * Apply a number of common transformations to clean data
# MAGIC   * Reshape and manipulate complex data using advanced built-in functions
# MAGIC * Intermediate programming experience with Delta Lake (create tables, perform complete and incremental updates, compact files, restore previous versions etc.)
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
