# Databricks notebook source

def insert_query(execution_time,test_id,env,testname,source_db,source_tb,target_db,target_tb,test_status):
    # unique_id = test_id+execution_time
    # print(unique_id)
    full_table_name = f"cert.test_status_db1.test_output_table"

    query = f"""
        INSERT INTO TABLE {full_table_name} VALUES ('{execution_time}','{test_id}','{env}','{testname}',
        '{source_db}','{source_tb}','{target_db}','{target_tb}','{test_status}')
    """

    spark.sql(query)
    print("Data inserted into test_output_table successfully.")

    # unique_id STRING (need to add)
    # query STRING (need to add)
    # execution_time timestamp,
    # test_id String,
    # environment STRING,
    # testname STRING,
    # source_database STRING,
    # source_tablename STRING,
    # target_database String,
    # target_tablename String,
    # test_status String

