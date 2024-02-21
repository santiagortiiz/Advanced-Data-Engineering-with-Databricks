# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-26c9d345-048e-466a-b8c8-6389458675dd
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Unit Test Users

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 0,--i18n-66ec1201-f320-4a84-8e93-7dcfd1cefcf9
# MAGIC %md
# MAGIC
# MAGIC ### Test silver table transformations

# COMMAND ----------

@dlt.table(
    # temporary=True,
    comment="Test: check clean table removes null ids and has correct count"
)
@dlt.expect_all({
    "keep_all_rows": "num_rows == 3",
    "null_ids_removed": "null_ids == 0"
})
def test_users_cdc_clean():
    return (
        dlt.read("users_cdc_clean")
            .select("*", F.col("alt_id").isNull().alias("alt_id_null"))
            .select(
                F.count("*").alias("num_rows"), 
                F.sum(F.col("alt_id_null").cast("int")).alias("null_ids"))
        )

# COMMAND ----------

# DBTITLE 0,--i18n-fae6af5c-6a47-4b8b-8014-c72223c1ab9c
# MAGIC %md
# MAGIC
# MAGIC ### Test primary key uniqueness

# COMMAND ----------

@dlt.table(
    # temporary=True,
    comment="Test: check that gold table only contains unique user id"
)
@dlt.expect_all({
    "pk_must_be_unique": "duplicate == 1"
})
def test_users_silver():
    return ( 
        dlt.read("users_silver")
            .groupby("alt_id")
            .agg(F.count("alt_id").alias("duplicate"))
    )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
