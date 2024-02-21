# Databricks notebook source
# DBTITLE 0,--i18n-e19759fe-e1e6-4d66-abee-fda09c7fab60
# MAGIC %md
# MAGIC ## Load Subset of Production Data
# MAGIC
# MAGIC This notebook uses the input parameters `source` and `start_date` to enable ingestion from different sources based on configuration values.

# COMMAND ----------

import dlt
production_db = spark.conf.get("production_db")
# start_date = spark.conf.get("start_date")

def create_bronze_subset(table):
    @dlt.table(
        name=table,
        table_properties={"quality": "bronze"},
        comment=f"Subset of raw production data for {table} table"
    )
    def bronze_subset():
        return (
            spark.read.table(f"{production_db}.{table}")
                # .filter(f"date > {start_date}")
                .limit(1000)
        )

# COMMAND ----------

bronze_topic_tables = ["bpm_bronze", "workouts_bronze", "users_cdc_bronze"]

for table in bronze_topic_tables:
    create_bronze_subset(table)

