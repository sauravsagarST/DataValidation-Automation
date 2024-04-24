# Databricks notebook source
# MAGIC %run ../utils/insert_ts

# COMMAND ----------

# MAGIC %run ../utils/util

# COMMAND ----------

def verify_duplicate_records(env, clientName, table_name, trg_col, suiteStartTime):

    full_table_name = f"`{env}`.`{clientName}`.`{table_name}`"
    testQuery = f"""SELECT (CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END) AS Test_status FROM (SELECT COUNT(*) AS cnt FROM ( SELECT COUNT(*) FROM {full_table_name} GROUP BY {trg_col} HAVING COUNT(*) > 1))"""

    result_df = spark.sql(testQuery)
    test_status = result_df.collect()[0]['Test_status']
    print(full_table_name+"  Duplicate Records Test for "+trg_col+" Status : "+test_status )

    insert_query(suiteStartTime,getCurrentTime(),getTestId(),env,'duplicate_records',clientName,'','',table_name,trg_col,testQuery,test_status)

    return test_status

# COMMAND ----------

def verify_null_records(env, clientName, table_name, trg_col, suiteStartTime):

    full_table_name = f"`{env}`.`{clientName}`.`{table_name}`"
    testQuery = f""" SELECT (CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END) AS Test_status FROM {full_table_name}WHERE {trg_col} IS NULL"""

    result_df = spark.sql(testQuery)
    test_status = result_df.collect()[0]['Test_status']
    print(full_table_name+"  Null Records Test for "+trg_col+" Status : "+test_status )

    insert_query(suiteStartTime,getCurrentTime(),getTestId(),env,'null_records',clientName,'','',table_name,trg_col,testQuery,test_status)
    
    return test_status

# COMMAND ----------

def Verify_full_load_row_count(env,clientName,target_table,suiteStartTime,date_column):

    full_table_name = f"`{env}`.`{clientName}`.`{target_table}`"
    dbricksQuery = f"""Select count(*) as row_count from {full_table_name} where {date_column} < '{getCurrentDate()}' """
    dbricksResult = spark.sql(dbricksQuery)
    dbricksCount = dbricksResult.collect()[0]['row_count'] 

    mySql_df = getMySqlData(env,clientName,target_table)
    mySql_df.createOrReplaceTempView("mysql_table")
    mySqlQuery = f"""SELECT count(*) as row_count FROM mysql_table where {date_column} < '{getCurrentDate()}' """
    sqlResult = spark.sql(mySqlQuery)
    mySqlCount = sqlResult.collect()[0]['row_count']
    source_table = target_table[7:]
    failCounts = ""
    if(dbricksCount == mySqlCount):
        test_status = "PASS"
        print("Full load Row Count Test between "+target_table+" and "+ source_table+" Status : "+test_status)
    else:
        test_status = "FAIL"
        failCounts = "Databricks Count(" +str(dbricksCount)+")  MySql Count("+str(mySqlCount)+")"
        print("Full load Row Count Test between "+target_table+" and "+ source_table+" Status : "+test_status+ ", " +failCounts)

    
    insert_query(suiteStartTime,getCurrentTime(),getTestId(),env,'full_load_row_count',clientName,source_table,failCounts,target_table,'','Databricks Query: '+dbricksQuery+' My Sql Query: '+mySqlQuery,test_status)

    return test_status

# COMMAND ----------

def Verify_full_load_row_count2(env,clientName,target_table,suiteStartTime):

    full_table_name = f"`{env}`.`{clientName}`.`{target_table}`"

    dbricksQuery = f"""select count(*) as row_count from {full_table_name}
    """
    dbricksResult = spark.sql(dbricksQuery)
    dbricksCount = dbricksResult.collect()[0]['row_count'] 

    mySql_df = getMySqlData(env,clientName,target_table)
    mySql_df.createOrReplaceTempView("mysql_table")
    
    mySqlQuery = f"""SELECT count(*) as row_count FROM mysql_table
    """
    sqlResult = spark.sql(mySqlQuery)
    mySqlCount = sqlResult.collect()[0]['row_count']
    source_table = target_table[7:]
    failCounts = ""
    if(dbricksCount == mySqlCount):
        test_status = "PASS"
        print("Full load Row Count Test between "+target_table+" and "+ source_table+" Status : "+test_status)
    else:
        test_status = "FAIL"
        failCounts = "Databricks Count(" +str(dbricksCount)+")  MySql Count("+str(mySqlCount)+")"
        print("Full load Row Count Test between "+target_table+" and "+ source_table+" Status : "+test_status+ ", " +failCounts)

    
    insert_query(suiteStartTime,getCurrentTime(),getTestId(),env,'full_load_row_count',clientName,source_table,failCounts,target_table,'','Databricks Query: '+dbricksQuery+' My Sql Query: '+mySqlQuery,test_status)

    return test_status

# COMMAND ----------

def verify_incremental_duplicate_records(env, clientName, table_name, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime):

    full_table_name = f"`{env}`.`{clientName}`.`{table_name}`"
    testQuery = f"""SELECT (CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END) AS Test_status FROM ( SELECT COUNT(*) AS cnt FROM (SELECT COUNT(*) FROM {full_table_name} where {primary_column} between {minIncrementalValue} and {maxIncrementalValue} GROUP BY {trg_col} HAVING COUNT(*) > 1))"""

    result_df = spark.sql(testQuery)
    test_status = result_df.collect()[0]['Test_status']
    print(full_table_name+"  Duplicate Records Test for "+trg_col+" Status : "+test_status )

    insert_query(suiteStartTime,getCurrentTime(),getTestId(),env,'duplicate_records_incremental',clientName,'','',table_name,trg_col,testQuery,test_status)

    return test_status

# COMMAND ----------

def verify_incremental_null_records(env, clientName, table_name, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime):
    full_table_name = f"`{env}`.`{clientName}`.`{table_name}`"

    testQuery = f"""SELECT (CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END) AS Test_status FROM {full_table_name} WHERE {primary_column} between {minIncrementalValue} and {maxIncrementalValue} and {trg_col} IS NULL"""

    result_df = spark.sql(testQuery)
    test_status = result_df.collect()[0]['Test_status']
    print(full_table_name+"  Null Records Test for "+trg_col+" Status : "+test_status )

    insert_query(suiteStartTime,getCurrentTime(),getTestId(),env,'null_records_incremental',clientName,'','',table_name,trg_col,testQuery,test_status)
    
    return test_status

# COMMAND ----------

def Verify_incremental_row_count(env, clientName, target_table, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column):

    full_table_name = f"`{env}`.`{clientName}`.`{target_table}`"

    dbricksQuery = f"""select count(*) as row_count from {full_table_name} where {primary_column} between {minincrementalValue} and {maxincrementalValue}
    """
    dbricksResult = spark.sql(dbricksQuery)
    dbricksCount = dbricksResult.collect()[0]['row_count'] 

    mySql_df = getMySqlData(env,clientName,target_table)
    mySql_df.createOrReplaceTempView("mysql_table")

    # Run SQL queries against the temporary view
    mySqlQuery = f"""SELECT count(*) as row_count FROM mysql_table where {primary_column} between {minincrementalValue} and {maxincrementalValue}
    """
    sqlResult = spark.sql(mySqlQuery)
    mySqlCount = sqlResult.collect()[0]['row_count']
    source_table = target_table[7:]
    failCounts = ""
    if(dbricksCount == mySqlCount):
        test_status = "PASS"
        print("Incremental Load Row Count Test between "+target_table+" and "+ source_table+" Status : "+test_status)
    else:
        test_status = "FAIL"
        failCounts = "Databricks Count(" +str(dbricksCount)+")  MySql Count("+str(mySqlCount)+")"
        print("Incremantal Row Count Test between "+target_table+" and "+ source_table+" Status : "+test_status+ ", " +failCounts)

    insert_query(suiteStartTime,getCurrentTime(),getTestId(),env,'incremental_row_count',clientName,source_table,failCounts,target_table,'','Databricks Query: '+dbricksQuery+' My Sql Query: '+mySqlQuery,test_status)

    return test_status

# COMMAND ----------

def verify_date_format(env, clientName, table_name, trg_col):
    suiteStartTime = getCurrentTime()
    full_table_name = f"{env}.{clientName}.{table_name}"
    expected_format = "yyyy-MM-dd'T'HH:mm:ss'Z'"

    testQuery = f"""
        SELECT (CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END) AS Test_status
        FROM {full_table_name}
        WHERE {trg_col} IS NOT NULL
        AND NOT regexp_like({trg_col}, '{expected_format}')
    """

    result_df = spark.sql(testQuery)
    test_status = result_df.collect()[0]['Test_status']
    print("'date_format_test' for " + table_name + " column: " + trg_col + ", Test status: " + test_status)
 
    insert_query(suiteStartTime, getCurrentTime(), getTestId(), env, 'date_format', '', '', clientName, table_name, trg_col, f"{testQuery}", test_status)

    return test_status

# COMMAND ----------

def incremental_row_count_test(env,clientName,target_table,suiteStartTime):

    full_table_name = f"`{env}`.`{clientName}`.`{table_name}`"

    query = f"""select incremental_column_name,incremental_column_value from cert.sfx_analytics.bronze_load_config where       
        app_id =  '{clientName}' and target='{target_table}'
    """
    rs = spark.sql(query)
    incremental_column_name = rs.collect()[0]['incremental_column_name']
    incremental_column_value = rs.collect()[0]['incremental_column_value']

    iquery = f"""select count(*) as incr_count from {full_table_name} where {incremental_column_name} > {incremental_column_value}
    """
    incr_rs = spark.sql(iquery)

    print(incr_rs.collect()[0]['incr_count'])

# COMMAND ----------

def verify_column_data_type(env,clientName,target_table,suiteStartTime):
    full_table_name = f"`{env}`.`{clientName}`.`{table_name}`"
    dbricksQuery = f"""SELECT column_name,data_type FROM system.information_schema.columns where table_name = 'bronze_users' and table_schema = 'xcloud'
    """
    dbricksResult = spark.sql(dbricksQuery)
    mySql_df = getMySqlData(env,"information_schema","bronze_columns")
    mysql_df.createOrReplaceTempView("mysql_tempTable")
    mySqlQuery = f"""SELECT column_name,data_type FROM mysql_tempTable where table_name = '{target_table}' and table_schema = '{clientName}'
    """
    sqlResult = spark.sql(mySqlQuery)

    comparison_results = []

    # Iterate over each row in the MySQL DataFrame
    for mysql_row in sqlResult.collect():
        column_name = mysql_row['column_name']
        mysql_data_type = mysql_row['data_type']

        # Find matching row in Databricks DataFrame
        dbricks_row = dbricksResult.filter(dbricksResult.column_name == column_name).collect()

        if len(dbricks_row) == 1:
            dbricks_data_type = dbricks_row[0]['data_type']
            match = mysql_data_type.lower() == dbricks_data_type.lower()
            comparison_results.append((column_name, mysql_data_type, dbricks_data_type, match))

    # Create DataFrame from comparison results
    comparison_df = spark.createDataFrame(comparison_results, ['column_name', 'mysql_data_type', 'dbricks_data_type', 'match'])
    return comparison_df

