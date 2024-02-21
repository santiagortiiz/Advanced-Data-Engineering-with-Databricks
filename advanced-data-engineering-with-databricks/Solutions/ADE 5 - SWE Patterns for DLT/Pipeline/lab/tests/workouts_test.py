# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-a5819b25-416b-4624-86f8-b5436f0e88b9
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Unit Test Workouts
# MAGIC
# MAGIC In this lab, you will learn to write unit test cases for validation to ensure the quality and accuracy of the data within tables based on specific criteria. You will validate table for quality check and removing duplicate records.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - How to ensure that the **"workouts_silver"** table contains no null entries and the right data.
# MAGIC - Validating that the **"workouts_completed"** table only contains unique workout IDs.
# MAGIC
# MAGIC **NOTE:** Make sure to run the driver notebook **ADE 5.5L - Develop and Test Pipelines Lab** inside source folder do not execute the cells in this notebook directly

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 0,--i18n-aeef42a7-ad99-4a90-a222-d6db5a0321e2
# MAGIC %md
# MAGIC
# MAGIC ### Test silver table transformations
# MAGIC
# MAGIC To test silver table transformations follow these steps:
# MAGIC 1. Define function with name as **`test_workouts_clean()`** that performs quality checks on a **workouts_silver** table.
# MAGIC 2. Read data from the **workouts_silver** table.
# MAGIC 3. Add a computed column named **workout_id_null** to indicate whether the workout ID is null.
# MAGIC 4. Validate the accuracy of row counts in table.
# MAGIC

# COMMAND ----------

# ANSWER

@dlt.table(
    # temporary=True,
    comment="Test: check silver table removes null workout ids and has correct count"
)
@dlt.expect_all({
    "keep_all_rows": "num_rows == 5",
    "null_ids_removed": "null_ids == 0"
})
def test_workouts_clean():
    return (
        dlt.read("workouts_silver")
            .select("*", F.col("workout_id").isNull().alias("workout_id_null"))
            .select(
                F.count("*").alias("num_rows"), 
                F.sum(F.col("workout_id_null").cast("int")).alias("null_ids"))
        )

# COMMAND ----------

# DBTITLE 0,--i18n-6d3a3629-4ba5-4135-89e6-e364c5261c96
# MAGIC %md
# MAGIC
# MAGIC ### Test primary key uniqueness
# MAGIC
# MAGIC To Test the **workouts_completed** table's primary key uniqueness
# MAGIC - Check if the **duplicate** count is equal to 1 for each unique workout ID in decoraters.
# MAGIC - Define function as **`test_workouts_completed()`** read workout_id and group operation on the data based on the **`workout_id`** column. 
# MAGIC - Count the occurrence of each **`workout_id`**", rename the result as **"duplicate"**.

# COMMAND ----------

# ANSWER

@dlt.table(
    # temporary=True,
    comment="Test: check that gold table only contains unique workout id"
)
@dlt.expect_all({
    "pk_must_be_unique": "duplicate == 1"
})
def test_workouts_completed():
    return ( 
        dlt.read("workouts_completed")
            .groupby("workout_id")
            .agg(F.count("workout_id").alias("duplicate"))
    )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
