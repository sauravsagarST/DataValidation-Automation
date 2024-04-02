# Databricks notebook source
from datetime import datetime

def verify_duplicate_records(database_name, schema_name, table_name, trg_col):
    # Constructing the fully qualified table name
    full_table_name = f"{database_name}.{schema_name}.{table_name}"
    
    exec_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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

    insertquery = f"""
        INSERT INTO TABLE cert.test_status_db1.test_output_table
        VALUES ('{exec_time}',
                'tc0016',
                '{database_name}',
                'duplicate_records',
                '',
                '',
                '{schema_name}',
                '{table_name}',
                '{test_status}')
    """
    spark.sql(insertquery)

    # insert_into_test_output_table(exec_time,'tc003',database_name,'duplicate_records','','',schema_name,table_name,test_status)

    return test_status


# COMMAND ----------

from datetime import datetime

def verify_null_records(database_name, schema_name, table_name, trg_col):
    
    full_table_name = f"{database_name}.{schema_name}.{table_name}"

    exec_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    query = f"""
        SELECT (CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END) AS Test_status
        FROM {full_table_name}
        WHERE {trg_col} IS NULL
    """

    # Executing the SQL query
    result_df = spark.sql(query)
    test_status = result_df.collect()[0]['Test_status']

    insertquery = f"""
        INSERT INTO TABLE cert.test_status_db1.test_output_table
        VALUES ('{exec_time}',
                'tc0016',
                '{database_name}',
                'null_records',
                '',
                '',
                '{schema_name}',
                '{table_name}',
                '{test_status}')
    """
    spark.sql(insertquery)

    # insert_into_test_output_table(exec_time,'tc003',database_name,'duplicate_records','','',schema_name,table_name,test_status)

    return test_status