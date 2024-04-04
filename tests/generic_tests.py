# Databricks notebook source
# MAGIC %run ../utils/insert_ts

# COMMAND ----------

# MAGIC %run ../utils/util

# COMMAND ----------

from datetime import datetime

def verify_duplicate_records(database_name, schema_name, table_name, trg_col):
    
    full_table_name = f"{database_name}.{schema_name}.{table_name}"
    
    query = f"""
        SELECT (CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END) AS Test_status
        FROM (
            SELECT COUNT(*) AS cnt
            FROM (
                SELECT COUNT(*) FROM {full_table_name} GROUP BY {trg_col} HAVING COUNT(*) > 1
            )
        )
    """

    # Executing the SQL query
    result_df = spark.sql(query)
    test_status = result_df.collect()[0]['Test_status']
    print("'duplicate_Record_test' for " + table_name + " column: " + trg_col + ", Test status: " + test_status)

    # Inserting test status into test_output_table 
    insert_query(executionTime(),testId(),database_name,'duplicate_records','','',schema_name,table_name,test_status)

    return test_status


# COMMAND ----------

from datetime import datetime

def verify_null_records(database_name, schema_name, table_name, trg_col):
    
    full_table_name = f"{database_name}.{schema_name}.{table_name}"

    query = f"""
        SELECT (CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END) AS Test_status
        FROM {full_table_name}
        WHERE {trg_col} IS NULL
    """

    # Executing the SQL query
    result_df = spark.sql(query)
    test_status = result_df.collect()[0]['Test_status']
    print("'null_records_test' for " + table_name + " column: " + trg_col + ", Test status: " + test_status)
    # Inserting test status into test_output_table 
    insert_query(executionTime(),testId(),database_name,'null_records','','',schema_name,table_name,test_status)

    return test_status