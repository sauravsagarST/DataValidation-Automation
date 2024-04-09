# Databricks notebook source
# MAGIC %run ../utils/insert_ts

# COMMAND ----------

# MAGIC %run ../utils/util

# COMMAND ----------

def verify_duplicate_records(env, clientName, table_name, trg_col, suiteStartTime):
    full_table_name = f"{env}.{clientName}.{table_name}"
    
    testQuery = f"""
        SELECT (CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END) AS Test_status
        FROM (
            SELECT COUNT(*) AS cnt
            FROM (
                SELECT COUNT(*) FROM {full_table_name} GROUP BY {trg_col} HAVING COUNT(*) > 1
            )
        )
    """

    result_df = spark.sql(testQuery)
    test_status = result_df.collect()[0]['Test_status']
    print("'duplicate_Record_test' for " + table_name + " column: " + trg_col + ", Test status: " + test_status)

    insert_query(suiteStartTime,getCurrentTime(),getTestId(),env,'duplicate_records','','',clientName,table_name,trg_col,'{testQuery}',test_status)

    return test_status


# COMMAND ----------

def verify_null_records(env, clientName, table_name, trg_col, suiteStartTime):
    full_table_name = f"{env}.{clientName}.{table_name}"

    testQuery = f"""
        SELECT (CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END) AS Test_status
        FROM {full_table_name}
        WHERE {trg_col} IS NULL
    """

    result_df = spark.sql(testQuery)
    test_status = result_df.collect()[0]['Test_status']
    print("'null_records_test' for " + table_name + " column: " + trg_col + ", Test status: " + test_status)
 
    insert_query(suiteStartTime,getCurrentTime(),getTestId(),env,'null_records','','',clientName,table_name,trg_col,'{testQuery}',test_status)

    return test_status

# COMMAND ----------

def Verify_full_load_row_count(env,clientName,target_table,suiteStartTime):
    full_table_name = f"{env}.{clientName}.{target_table}"

    dbricksQuery = f"""select count(*) as row_count from {full_table_name}
    """
    dbricksResult = spark.sql(dbricksQuery)
    dbricksCount = dbricksResult.collect()[0]['row_count'] 

    mySql_df = getMySqlData(env,clientName,target_table)
    mySql_df.createOrReplaceTempView("mysql_table")
   
    # Run SQL queries against the temporary view
    mySqlQuery = f"""SELECT count(*) as row_count FROM mysql_table
    """
    sqlResult = spark.sql(mySqlQuery)
    mySqlCount = sqlResult.collect()[0]['row_count']
    if(dbricksCount == mySqlCount):
        test_status = "PASS"
    else:
        test_status = "FAIL"

    source_table = target_table[7:]
    print("'full_load_row_count_test' target table " + target_table + " and source table: " + source_table + ", Test status: " + test_status)
   
    insert_query(suiteStartTime,getCurrentTime(),getTestId(),env,'full_load_row_count','',source_table,clientName,target_table,'','{testQuery}',test_status)

    return test_status
