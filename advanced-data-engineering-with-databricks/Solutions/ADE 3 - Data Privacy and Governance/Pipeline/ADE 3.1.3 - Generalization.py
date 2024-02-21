# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-45cf18cb-a4c3-4328-a969-b899a376efb0
# MAGIC %md
# MAGIC
# MAGIC # Generalize PII
# MAGIC
# MAGIC This lesson explores approaches for reducing risk of PII leakage while working with potentially sensitive information for analytics and reporting.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, students will be able to:
# MAGIC - Create binned tables to generalize data and obscure PII

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

source = spark.conf.get("source")

# COMMAND ----------

# DBTITLE 0,--i18n-91af70ec-868f-4ae7-a9e8-0a0a90e92279
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Generalize PII in Aggregate Tables
# MAGIC
# MAGIC Another approach to reducing chance of exposing PII is only providing access to data at a less specific level. In this section, we'll assign users to age bins while maintaining their gender, city, and state information. This will provide sufficient demographic information to build comparative dashboards without revealing specific user identity.
# MAGIC
# MAGIC Here we're just defining custom logic for replacing values with manually-specified labels.
# MAGIC
# MAGIC **NOTE:** As currently implemented, each time this logic is processed, all records will be overwritten with newly calculated values. To decrease chances of identifying birth date at binned boundaries, random noise could be added to the values used to calculate age bins (generally keeping age bins accurate, but reducing the likelihood of transitioning a user to a new bin on their exact birthday).

# COMMAND ----------

def age_bins(dob_col):
    age_col = F.floor(F.months_between(F.current_date(), dob_col) / 12).alias("age")
    return (
        F.when((age_col < 18), "under 18")
        .when((age_col >= 18) & (age_col < 25), "18-25")
        .when((age_col >= 25) & (age_col < 35), "25-35")
        .when((age_col >= 35) & (age_col < 45), "35-45")
        .when((age_col >= 45) & (age_col < 55), "45-55")
        .when((age_col >= 55) & (age_col < 65), "55-65")
        .when((age_col >= 65) & (age_col < 75), "65-75")
        .when((age_col >= 75) & (age_col < 85), "75-85")
        .when((age_col >= 85) & (age_col < 95), "85-95")
        .when((age_col >= 95), "95+")
        .otherwise("invalid age")
        .alias("age")
    )


@dlt.table
def user_bins():
    return (
        dlt.read("users")
        .join(dlt.read("user_lookup").select("alt_id", "user_id"), ["alt_id"], "left")
        .select("user_id", age_bins(F.col("dob")), "gender", "city", "state")
    )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
