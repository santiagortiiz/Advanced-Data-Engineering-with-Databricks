# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-32096204-ce9c-43ed-b655-34d3ff7e9163
# MAGIC %md
# MAGIC ## Performance Optimization
# MAGIC This module is part of the Data Engineer Learning Path by Databricks Academy.
# MAGIC
# MAGIC ### Lessons
# MAGIC #### Partitioning Data <br>
# MAGIC Lecture: Designing the Foundation (slides available in .pdf) <br>
# MAGIC Lecture: Code Optimization (slides available in .pdf) <br>
# MAGIC Lecture: Spark Architecture (slides available in .pdf) <br>
# MAGIC Slides: Data Files in a Data Lake [Exp #8973-S - Scanning](https://www.databricks.training/spark-ui-simulator/experiment-8973/v002-S/index.html) <br>
# MAGIC Slides: Partitioning in Delta Lake [Exp #2934-S - Predicate Pushdown Comparison](https://www.databricks.training/spark-ui-simulator/experiment-2934/v002-S/index.html) <br>
# MAGIC Slides: Index Delta Optimize and File Size Tuning [Exp #8923-S - Read Tiny Files](https://www.databricks.training/spark-ui-simulator/experiment-8923/v002-S/index.html) <br>
# MAGIC Slides: Storage - Delta Lake Z-Ordering <br>
# MAGIC &emsp;[Exp #2934-S - Predicate Pushdown Comparison](https://www.databricks.training/spark-ui-simulator/experiment-2934/v002-S/index.html) <br>
# MAGIC &emsp;[Exp #1337-S - Z-Order Haystack Query](https://www.databricks.training/spark-ui-simulator/experiment-1337/v002-S/index.html) <br>
# MAGIC
# MAGIC #### Assessing and Debugging Applications <br>
# MAGIC Slides: Tuning Shuffle Partitions [Exp #2653-S - Shuffle Partitions](https://www.databricks.training/spark-ui-simulator/experiment-2653/v002-S/index.html) <br>
# MAGIC Slides: Apache Spark Join Types [Exp #9157-S - Shuffle Hash Join](https://www.databricks.training/spark-ui-simulator/experiment-9157/v005-S/index.html) <br>
# MAGIC Slides: Join Optimizations [Exp #3799A-S - Filtered Join, Standard](https://www.databricks.training/spark-ui-simulator/experiment-3799A/v002-S/index.html) <br>
# MAGIC Slides: Skew Join Optimizations [Exp #1596-S - Skew Joins](https://www.databricks.training/spark-ui-simulator/experiment-1596/v002-S/index.html) <br>
# MAGIC Slides: Spill [Exp #6518-S - Spill](https://www.databricks.training/spark-ui-simulator/experiment-6518/v002-S/index.html) <br>
# MAGIC Slides: Unhandled Skew [Exp #2755-P - Unhandled Skew](https://www.databricks.training/spark-ui-simulator/experiment-2755/v004-P/index.html) <br>
# MAGIC Slides: Serialization 
# MAGIC &emsp;[Exp #5980-PS - Benchmarking](https://www.databricks.training/spark-ui-simulator/experiment-5980/v002-S/index.html)<br>
# MAGIC &emsp;[Exp #4538-S - UDFs](https://www.databricks.training/spark-ui-simulator/experiment-4538/v002-S/index.html) <br>
# MAGIC &emsp;[Exp #4538-P - UDFs](https://www.databricks.training/spark-ui-simulator/experiment-4538/v002-P/index.html) <br>
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

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
