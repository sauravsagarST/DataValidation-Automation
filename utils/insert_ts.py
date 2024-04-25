# Databricks notebook source
def insert_query(suiteStartTime,execution_time,test_id,env,testname,source_client,source_tb,target_client,target_tb,trg_column,testQuery,test_status):
    
    full_table_name = f"{env}.sfx_analytics.test_output_table"

    query = f"""INSERT INTO TABLE {full_table_name} VALUES('{suiteStartTime}','{execution_time}','{test_id}','{env}','{testname}','{source_client}','{source_tb}','{target_client}','{target_tb}','{trg_column}',"{testQuery}",'{test_status}')"""
   
    spark.sql(query)
    # print(query)

    # SUITE_START_TIME STRING (need to add)
    # TEST_START_TIME timestamp,
    # TEST_ID String,
    # ENVIRONMENT STRING,
    # TEST_NAME STRING,
    # SOURCE_CLIENT STRING,
    # SOURCE_TABLE_NAME STRING,
    # TARGET_CLIENT String,
    # TARGET_TABLE_NAME String,
    # TEST_QUERY STRING (need to add)
    # TEST_STATUS String
