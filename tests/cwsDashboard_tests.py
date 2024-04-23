# Databricks notebook source
# MAGIC %run ../utils/insert_ts

# COMMAND ----------

# MAGIC %run ../utils/util

# COMMAND ----------

def s3fileRowCounter(gaPath,client):
    client_dbname = "dbname="+client+"/"
    file_RowCount = 0
    clientDirectoryList = dbutils.fs.ls(gaPath)
    for clientDirectory in clientDirectoryList:
        if(clientDirectory[1] == client_dbname):
            filesList = dbutils.fs.ls(clientDirectory[0])
            for files in filesList:
                l = spark.read.text(files[0],lineSep="\n")
                files_RowCount = files_RowCount + l.count()
    return file_RowCount

# COMMAND ----------

def googleAnalytics_GaContent_RowCount(env,clientName,target_table,suiteStartTime):
    full_table_name = f"{env}.sfx_analytics.{target_table}"

    dbricksQuery = f"""select count(*) as row_count from {full_table_name} where dbname = '{clientName}'
    """
    dbricksResult = spark.sql(dbricksQuery)
    dbricksCount = dbricksResult.collect()[0]['row_count'] 

    s3rowCount = s3fileRowCounter(googleAnalytics_GaContentPath,clientName)
    source_table = target_table[7:]
    if(dbricksCount == s3rowCount):
        test_status = "PASS"
        print("'googleAnalytics_GaContent_RowCount_test' target table " + target_table + " and source table: " + source_table + ", Test status: " + test_status)
    else:
        test_status = "FAIL"
        print("'googleAnalytics_GaContent_RowCount_test' target table " + target_table + " and source table: " + source_table + ", Test status: " + test_status + ",  Databricks_count(" +str(dbricksCount)+")  S3_count("+str(s3rowCount)+")" )

    # insert_query(suiteStartTime,getCurrentTime(),getTestId(),env,'googleAnalytics_GaContent_RowCount','',source_table,clientName,target_table,'','{testQuery}',test_status)

    return test_status

# COMMAND ----------

def googleAnalytics_GaLocation_RowCount(env,clientName,target_table,suiteStartTime):
    full_table_name = f"{env}.sfx_analytics.{target_table}"

    dbricksQuery = f"""select count(*) as row_count from {full_table_name} where dbname = '{clientName}'
    """
    dbricksResult = spark.sql(dbricksQuery)
    dbricksCount = dbricksResult.collect()[0]['row_count'] 

    s3rowCount = s3fileRowCounter(googleAnalytics_GaLocationPath,clientName)
    source_table = target_table[7:]
    if(dbricksCount == s3rowCount):
        test_status = "PASS"
        print("'googleAnalytics_GaLocation_RowCount_test' target table " + target_table + " and source table: " + source_table + ", Test status: " + test_status)
    else:
        test_status = "FAIL"
        print("'googleAnalytics_GaLocation_RowCount_test' target table " + target_table + " and source table: " + source_table + ", Test status: " + test_status + ",  Databricks_count(" +str(dbricksCount)+")  S3_count("+str(s3rowCount)+")" )

    # insert_query(suiteStartTime,getCurrentTime(),getTestId(),env,'googleAnalytics_GaLocation_RowCount','',source_table,clientName,target_table,'','{testQuery}',test_status)

    return test_status

# COMMAND ----------

def googleAnalytics_GaSources_RowCount(env,clientName,target_table,suiteStartTime):
    full_table_name = f"{env}.sfx_analytics.{target_table}"

    dbricksQuery = f"""select count(*) as row_count from {full_table_name} where dbname = '{clientName}'
    """
    dbricksResult = spark.sql(dbricksQuery)
    dbricksCount = dbricksResult.collect()[0]['row_count'] 

    s3rowCount = s3fileRowCounter(googleAnalytics_GaSourcesPath,clientName)
    source_table = target_table[7:]
    if(dbricksCount == s3rowCount):
        test_status = "PASS"
        print("'googleAnalytics_GaSources_RowCount_test' target table " + target_table + " and source table: " + source_table + ", Test status: " + test_status)
    else:
        test_status = "FAIL"
        print("'googleAnalytics_GaSources_RowCount_test' target table " + target_table + " and source table: " + source_table + ", Test status: " + test_status + ",  Databricks_count(" +str(dbricksCount)+")  S3_count("+str(s3rowCount)+")" )

    # insert_query(suiteStartTime,getCurrentTime(),getTestId(),env,'googleAnalytics_GaSources_RowCount','',source_table,clientName,target_table,'','{testQuery}',test_status)

    return test_status

# COMMAND ----------

def googleAnalytics_GaVisitors_RowCount(env,clientName,target_table,suiteStartTime):
    full_table_name = f"{env}.sfx_analytics.{target_table}"

    dbricksQuery = f"""select count(*) as row_count from {full_table_name} where dbname = '{clientName}'
    """
    dbricksResult = spark.sql(dbricksQuery)
    dbricksCount = dbricksResult.collect()[0]['row_count'] 

    s3rowCount = s3fileRowCounter(googleAnalytics4_GaVisitorsPath,clientName)
    source_table = target_table[7:]
    if(dbricksCount == s3rowCount):
        test_status = "PASS"
        print("'googleAnalytics_GaVisitors_RowCount_test' target table " + target_table + " and source table: " + source_table + ", Test status: " + test_status)
    else:
        test_status = "FAIL"
        print("'googleAnalytics_GaVisitors_RowCount_test' target table " + target_table + " and source table: " + source_table + ", Test status: " + test_status + ",  Databricks_count(" +str(dbricksCount)+")  S3_count("+str(s3rowCount)+")" )

    # insert_query(suiteStartTime,getCurrentTime(),getTestId(),env,'googleAnalytics_GaVisitors_RowCount','',source_table,clientName,target_table,'','{testQuery}',test_status)

    return test_status

# COMMAND ----------

def googleAnalytics4_Ga4_Content_RowCount(env,clientName,target_table,suiteStartTime):
    full_table_name = f"{env}.sfx_analytics.{target_table}"

    dbricksQuery = f"""select count(*) as row_count from {full_table_name} where dbname = '{clientName}'
    """
    dbricksResult = spark.sql(dbricksQuery)
    dbricksCount = dbricksResult.collect()[0]['row_count'] 

    s3rowCount = s3fileRowCounter(googleAnalytics4_GaContentPath,clientName)
    source_table = target_table[7:]
    if(dbricksCount == s3rowCount):
        test_status = "PASS"
        print("'googleAnalytics4_Ga4_Content_RowCount_test' target table " + target_table + " and source table: " + source_table + ", Test status: " + test_status)
    else:
        test_status = "FAIL"
        print("'googleAnalytics4_Ga4_Content_RowCount_test' target table " + target_table + " and source table: " + source_table + ", Test status: " + test_status + ",  Databricks_count(" +str(dbricksCount)+")  S3_count("+str(s3rowCount)+")" )

    # insert_query(suiteStartTime,getCurrentTime(),getTestId(),env,'googleAnalytics4_Ga4_Content_RowCount','',source_table,clientName,target_table,'','{testQuery}',test_status)

    return test_status

# COMMAND ----------

def googleAnalytics4_Ga4_Location_RowCount(env,clientName,target_table,suiteStartTime):
    full_table_name = f"{env}.sfx_analytics.{target_table}"

    dbricksQuery = f"""select count(*) as row_count from {full_table_name} where dbname = '{clientName}'
    """
    dbricksResult = spark.sql(dbricksQuery)
    dbricksCount = dbricksResult.collect()[0]['row_count'] 

    s3rowCount = s3fileRowCounter(googleAnalytics4_GaLocationPath,clientName)
    source_table = target_table[7:]
    if(dbricksCount == s3rowCount):
        test_status = "PASS"
        print("'googleAnalytics4_Ga4_Location_RowCount_test' target table " + target_table + " and source table: " + source_table + ", Test status: " + test_status)
    else:
        test_status = "FAIL"
        print("'googleAnalytics4_Ga4_Location_RowCount_test' target table " + target_table + " and source table: " + source_table + ", Test status: " + test_status + ",  Databricks_count(" +str(dbricksCount)+")  S3_count("+str(s3rowCount)+")" )

    # insert_query(suiteStartTime,getCurrentTime(),getTestId(),env,'googleAnalytics4_Ga4_Location_RowCount','',source_table,clientName,target_table,'','{testQuery}',test_status)

    return test_status

# COMMAND ----------

def googleAnalytics4_Ga4_Sources_RowCount(env,clientName,target_table,suiteStartTime):
    full_table_name = f"{env}.sfx_analytics.{target_table}"

    dbricksQuery = f"""select count(*) as row_count from {full_table_name} where dbname = '{clientName}'
    """
    dbricksResult = spark.sql(dbricksQuery)
    dbricksCount = dbricksResult.collect()[0]['row_count'] 

    s3rowCount = s3fileRowCounter(googleAnalytics4_GaSourcesPath,clientName)
    source_table = target_table[7:]
    if(dbricksCount == s3rowCount):
        test_status = "PASS"
        print("'googleAnalytics4_Ga4_Sources_RowCount_test' target table " + target_table + " and source table: " + source_table + ", Test status: " + test_status)
    else:
        test_status = "FAIL"
        print("'googleAnalytics4_Ga4_Sources_RowCount_test' target table " + target_table + " and source table: " + source_table + ", Test status: " + test_status + ",  Databricks_count(" +str(dbricksCount)+")  S3_count("+str(s3rowCount)+")" )

    # insert_query(suiteStartTime,getCurrentTime(),getTestId(),env,'googleAnalytics4_Ga4_Sources_RowCount','',source_table,clientName,target_table,'','{testQuery}',test_status)

    return test_status

# COMMAND ----------

def googleAnalytics4_Ga4_Visitors_RowCount(env,clientName,target_table,suiteStartTime):
    full_table_name = f"{env}.sfx_analytics.{target_table}"

    dbricksQuery = f"""select count(*) as row_count from {full_table_name} where dbname = '{clientName}'
    """
    dbricksResult = spark.sql(dbricksQuery)
    dbricksCount = dbricksResult.collect()[0]['row_count'] 

    s3rowCount = s3fileRowCounter(googleAnalytics4_GaVisitorsPath,clientName)
    source_table = target_table[7:]
    if(dbricksCount == s3rowCount):
        test_status = "PASS"
        print("'googleAnalytics4_Ga4_Visitors_RowCount_test' target table " + target_table + " and source table: " + source_table + ", Test status: " + test_status)
    else:
        test_status = "FAIL"
        print("'googleAnalytics4_Ga4_Visitors_RowCount_test' target table " + target_table + " and source table: " + source_table + ", Test status: " + test_status + ",  Databricks_count(" +str(dbricksCount)+")  S3_count("+str(s3rowCount)+")" )

    # insert_query(suiteStartTime,getCurrentTime(),getTestId(),env,'googleAnalytics4_Ga4_Visitors_RowCount','',source_table,clientName,target_table,'','{testQuery}',test_status)

    return test_status

# COMMAND ----------

def verify_S3_data_date_length(env, clientName, table_name, trg_col, suiteStartTime):
    full_table_name = f"{env}.sfx_analytics.{table_name}"
    
    testQuery = f"""
        SELECT (CASE WHEN LENGTH({trg_col}) = 8 THEN 'PASS' ELSE 'FAIL' END) AS Test_status
        FROM {full_table_name} where where dbname = '{clientName}'
    """

    result_df = spark.sql(testQuery)
    test_status = result_df.collect()[0]['Test_status']
    print("'date_length_test' for " + table_name + " column: " + trg_col + ", Test status: " + test_status)

    insert_query(suiteStartTime,getCurrentTime(),getTestId(),env,'date_format_length','','',clientName,table_name,trg_col,'{testQuery}',test_status)

    return test_status


# COMMAND ----------

def verify_S3_data_date_day_month_range(env, clientName, table_name, trg_col, suiteStartTime):
    full_table_name = f"{env}.sfx_analytics.{table_name}"
    
    testQuery = f"""
        SELECT (CASE 
                    WHEN SUBSTRING({trg_col}, 7, 2) BETWEEN '01' AND '31' AND 
                         SUBSTRING({trg_col}, 5, 2) BETWEEN '01' AND '12' THEN 'PASS' 
                    ELSE 'FAIL' 
                END) AS Test_status
        FROM {full_table_name}
    """

    result_df = spark.sql(testQuery)
    test_status = result_df.collect()[0]['Test_status']
    print("'date_day_month_range_test' for " + table_name + " column: " + trg_col + ", Test status: " + test_status)

    insert_query(suiteStartTime, getCurrentTime(), getTestId(), env, 'date_day_month_range', '', '', clientName, table_name, trg_col, '{testQuery}', test_status)

    return test_status

