# Databricks notebook source
# MAGIC %run ./Classroom-Setup-01-Common

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def validate_1_1(self, df):
    suite = self.tests.new("1.2L-1.1")

    suite.test_true(actual_value=lambda: df.isStreaming, description="The query is streaming")
    
    columns = ['order_id', 'email', 'transaction_timestamp', 'total_item_quantity', 'purchase_revenue_in_usd', 'unique_items', 'items']
    suite.test_sequence(actual_value=lambda: df.columns, 
                        expected_value=columns,
                        test_column_order=False,
                        description=f"DataFrame contains all {len(columns)} columns",
                        hint="Found [[ACTUAL_VALUE]]")
    
    suite.display_results()
    assert suite.passed, "One or more tests failed."

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def validate_2_1(self, schema: StructType):

    suite = self.tests.new("1.2L-2.1")

    suite.test_equals(
        actual_value=lambda: type(schema),
        expected_value=StructType,
        description="Schema is of type StructType",
        hint="Found [[ACTUAL_VALUE]]",
    )

    suite.test_length(
        lambda: schema.fieldNames(),
        expected_length=7,
        description="Schema contains seven fields",
        hint="Found [[LEN_ACTUAL_VALUE]]: [[ACTUAL_VALUE]]",
    )

    suite.test_schema_field(lambda: schema, "order_id", "LongType", None)
    suite.test_schema_field(lambda: schema, "email", "StringType", None)
    suite.test_schema_field(lambda: schema, "transaction_timestamp", "LongType", None)
    suite.test_schema_field(lambda: schema, "total_item_quantity", "LongType", None)
    suite.test_schema_field(lambda: schema, "purchase_revenue_in_usd", "DoubleType", None)
    suite.test_schema_field(lambda: schema, "unique_items", "LongType", None)
    suite.test_schema_field(lambda: schema, "items", "StructType", None)

    suite.display_results()
    assert suite.passed, "One or more tests failed."

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def validate_3_1(self, query):
    suite = self.tests.new("1.2L-3.1")

    suite.test_true(actual_value=lambda: query.isActive, description="The query is active")

    suite.test_equals(lambda: coupon_sales_query.lastProgress["name"], "coupon_sales",
                      description="The query name is \"coupon_sales\".")
    
    coupons_output_path = f"{DA.paths.working_dir}/coupon-sales/output"
    suite.test(actual_value=lambda: None, test_function=lambda: len(dbutils.fs.ls(coupons_output_path)) > 0,
               description=f"Found at least one file in .../coupon-sales/output")

    coupons_checkpoint_path = f"{DA.paths.checkpoints}/coupon-sales"
    suite.test(actual_value=lambda: None, test_function=lambda: len(dbutils.fs.ls(coupons_checkpoint_path)) > 0,
               description=f"Found at least one file in .../coupon-sales")

    suite.display_results()
    assert suite.passed, "One or more tests failed."


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def validate_4_1(self, query_id, query_status):
    suite = self.tests.new("1.2L-4.1")

    suite.test_sequence(actual_value=lambda: query_status.keys(),
                        expected_value=["message", "isDataAvailable", "isTriggerActive"],
                        test_column_order=False,
                        description="Valid status value.")

    suite.test_equals(lambda: type(query_id), str, description="Valid query_id value.")
    
    suite.display_results()
    assert suite.passed, "One or more tests failed."

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def validate_5_1(self, query):
    suite = self.tests.new("1.2L-5.1")

    suite.test_false(actual_value=lambda: query.isActive, description="The query is not active")

    suite.display_results()
    assert suite.passed, "One or more tests failed."

