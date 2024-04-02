# Databricks notebook source
# Define the SQL query to create the test_output_table
create_table_query = """
CREATE TABLE IF NOT EXISTS cert.test_satus_db1.test_output_table (
    execution_time TIMESTAMP,
    test_id STRING,
    environment STRING,
    testname STRING,
    source_database STRING,
    source_tablename STRING,
    target_database STRING,
    target_tablename STRING,
    test_status STRING
)
"""

# Execute the SQL query to create the table
spark.sql(create_table_query)

# Print message indicating table creation status
print("test_output_table created successfully.")