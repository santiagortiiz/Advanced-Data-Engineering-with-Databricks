# Databricks notebook source
# MAGIC %pip install --quiet --disable-pip-version-check https://github.com/databricks-academy/advanced-data-engineering-with-databricks-demo-test-data-setup/releases/download/v0.0.5/data_setup-0.0.5-py3-none-any.whl

# COMMAND ----------

# DBTITLE 0,--i18n-e6536a54-3ef8-4f95-95d1-846f22c6c315
# MAGIC %md
# MAGIC
# MAGIC ## Pipeline Dependencies
# MAGIC Import module from the package installed above to access our sample datasets.

# COMMAND ----------

import data_setup.test_data_setup as setup
data = setup.test_data()

# COMMAND ----------

# DBTITLE 0,--i18n-42a287c1-91a2-4ea5-8575-a1cbe1008677
# MAGIC %md
# MAGIC ## Load Sample Datasets

# COMMAND ----------

import dlt
import pandas as pd

def create_bronze_sample(table_name, sample_json, json_schema):
    @dlt.table(
        name=table_name,
        table_properties={"quality": "bronze"},
        comment=f"Sample data for {table_name} records for unit testing"
    )
    def sample_bronze():
        pdf = pd.DataFrame(sample_json.split("\n"), columns=["data"])
        return (
          spark.createDataFrame(pdf)
                  .selectExpr(f"from_json(data, '{json_schema}') AS json")
                  .select("json.*")
        )

# COMMAND ----------

bronze_tables_config = {
    "bpm_bronze": {
        "sample_json": data.bpm_json,
        "json_schema": "device_id LONG, time TIMESTAMP, heartrate DOUBLE"
    },
    "workouts_bronze": {
        "sample_json": data.workouts_json,
        "json_schema": "user_id INT, workout_id INT, timestamp FLOAT, action STRING, session_id INT"
    },
    "users_cdc_bronze": {
        "sample_json": data.users_cdc_json,
        "json_schema": "user_id LONG, update_type STRING, timestamp FLOAT, dob STRING, sex STRING, gender STRING, first_name STRING, last_name STRING, address STRUCT<street_address: STRING, city: STRING, state: STRING, zip: INT>"
    }
} 

# COMMAND ----------

for table, config in bronze_tables_config.items():
    create_bronze_sample(table, config["sample_json"], config["json_schema"])

