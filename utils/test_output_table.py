# Databricks notebook source
# Define the SQL query to create the test_output_table
create_table_query = """
CREATE TABLE IF NOT EXISTS cert.sfx_analytics.test_output_table (
    SUITE_START_TIME TIMESTAMP,
    TEST_START_TIME TIMESTAMP,
    TEST_ID STRING,
    ENVIRONMENT STRING,
    TEST_NAME STRING,
    SOURCE_CLIENT STRING,
    SOURCE_TABLE_NAME STRING,
    TARGET_CLIENT STRING,
    TARGET_TABLE_NAME STRING,
    TARGET_COLUMN STRING,
    TEST_QUERY STRING,
    TEST_STATUS STRING
)
"""

# Execute the SQL query to create the table
spark.sql(create_table_query)

# Print message indicating table creation status
print("test_output_table created successfully.")