# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

# MAGIC %run ../../Includes/_stream_factory

# COMMAND ----------

# MAGIC %run ../../Includes/_pipeline_config

# COMMAND ----------

def init_DA(name, reset=True, pipeline=False):

    lesson_config = LessonConfig(name=name, 
                    create_schema=True,
                    create_catalog = False,
                    requires_uc = False,
                    installing_datasets = True,
                    enable_streaming_support = False,
                    enable_ml_support = False)
    DA = DBAcademyHelper(course_config, lesson_config)
    if reset: DA.reset_lesson()
    DA.init()

    if pipeline:
        DA.paths.stream_source = f"{DA.paths.working_dir}/stream_source"
        DA.paths.storage_location = f"{DA.paths.working_dir}/storage_location"
        DA.pipeline_name = f"{DA.unique_name(sep='-')}: ETL Pipeline - {name.upper()}"
        DA.lookup_db = f"{DA.unique_name(sep='_')}_lookup"

        if reset:  # create lookup tables
            import pandas as pd

            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DA.lookup_db}")
            DA.clone_source_table(f"{DA.lookup_db}.date_lookup", source_name="date-lookup")
            DA.clone_source_table(f"{DA.lookup_db}.user_lookup", source_name="user-lookup")

            df = spark.createDataFrame(pd.DataFrame([
                ("device_id_not_null", "device_id IS NOT NULL", "validity", "bpm"),
                ("device_id_valid_range", "device_id > 110000", "validity", "bpm"),
                ("heartrate_not_null", "heartrate IS NOT NULL", "validity", "bpm"),
                ("user_id_not_null", "user_id IS NOT NULL", "validity", "workout"),
                ("workout_id_not_null", "workout_id IS NOT NULL", "validity", "workout"),
                ("action_not_null","action IS NOT NULL","validity","workout"),
                ("user_id_not_null","user_id IS NOT NULL","validity","user_info"),
                ("update_type_not_null","update_type IS NOT NULL","validity","user_info")
            ], columns=["name", "condition", "tag", "topic"]))
            df.write.mode("overwrite").saveAsTable(f"{DA.lookup_db}.rules")
        
    return DA

None

# COMMAND ----------

import os

@DBAcademyHelper.monkey_patch
def load_credentials(self):
    import configparser
    c = configparser.ConfigParser()
    c.read(filenames=f"var/credentials.cfg")
    try:
        token = c.get('DEFAULT', 'db_token')
        host = c.get('DEFAULT', 'db_instance')
        os.environ["DATABRICKS_HOST"] = host
        os.environ["DATABRICKS_TOKEN"] = token
    except:
        token = ''
        host = ''

    return token, host    

@DBAcademyHelper.monkey_patch
def get_credentials(self):
    import ipywidgets as widgets

    (current_token, current_host) = self.load_credentials()

    @widgets.interact(host=widgets.Text(description='Host:',
                                        placeholder='Paste workspace URL here',
                                        value = current_host,
                                        continuous_update=False),
                      token=widgets.Password(description='Token:',
                                             placeholder='Paste PAT here',
                                             value = current_token,
                                             continuous_update=False)
    )
    def _f(host='', token=''):
        from urllib.parse import urlparse,urlunsplit

        u = urlparse(host)
        host = urlunsplit((u.scheme, u.netloc, '', '', ''))

        if host and token:
            os.environ["DATABRICKS_HOST"] = host
            os.environ["DATABRICKS_TOKEN"] = token

            contents = f"""
[DEFAULT]
db_token = {token}
db_instance = {host}
            """
            os.makedirs('var', exist_ok=True)
            with open("var/credentials.cfg", "w") as f:
                print(f"Credentials stored ({f.write(contents)} bytes written).")

None

