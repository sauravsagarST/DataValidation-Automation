# Databricks notebook source
# MAGIC %run ../scripts/bronze/bronze_dictionary

# COMMAND ----------

from datetime import datetime

def getMySqlHost():
    return mySqlHost


def getCurrentTime():
    curr_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return curr_time


def getTestId():
    tId = "TC"+datetime.now().strftime("%Y-%m-%d_%H:%M:%S:%f")
    return tId


def getDuplicateTestTargetColumns(trg_tb):
    dict_name = trg_tb+'_dict'
    target_dict = globals()[dict_name]
    return target_dict.get('duplicate_tc', [])


def getNullTestTargetColumns(trg_tb):
    dict_name = trg_tb+'_dict'
    target_dict = globals()[dict_name]
    return target_dict.get('null_tc', [])


def getTargetTableList(app_id):
    query = f"""
        SELECT DISTINCT target FROM cert.sfx_analytics.bronze_load_config WHERE app_id = "{app_id}"
    """
    print(query)
    result_df = spark.sql(query)
    trg_table_list = [row.target for row in result_df.collect()]
    return trg_table_list


def deleteOldTestOutputRecords():
    from datetime import datetime, timedelta
    ten_days_ago = datetime.now() - timedelta(days=10)
    formatted_date = ten_days_ago.strftime('%Y-%m-%d')
    deleteQuery = f"DELETE FROM cert.sfx_analytics.test_output_table WHERE SUITE_START_TIME < '{formatted_date}'"
    spark.sql(deleteQuery)
    print("Data older than 10 days has been successfully deleted.")


def getMySqlData(env,client,trg_tb):
    src_tb = trg_tb[7:]
    db_host = getMySqlHost()
    scope = f"{env}-credentials"
    # Reading credentials from Secret Scope
    db_username = dbutils.secrets.get(scope=f"{scope}", key="db-user")
    db_password = dbutils.secrets.get(scope=f"{scope}", key="db-password")

    mysql_df = spark.read \
                .format("jdbc") \
                .option("url", db_host) \
                .option("dbtable",f"{client}.{src_tb}") \
                .option("user", db_username) \
                .option("password", db_password) \
                .load()
    mysql_df.createOrReplaceTempView("mysql_tempTable")
    return mysql_df

# load data from s3 to bronze(gaVisitors)
def getS3Data(env,client,trg_tb):
    input_path = dbutils.widgets.get("input_path") # source file path in S3 - "s3://hodes-external-data/google-analytics/archive-clean/ga_visitors/"
    env = dbutils.widgets.get("env")
    output_table_name = dbutils.widgets.get("output_table") #bronze_ga_visitors
    output_table = f"{env}.sfx_analytics.{output_table_name}" # target table name in databricks
    checkpoint_location = dbutils.widgets.get("checkpoint_location")  # checkpoint path of the table "/FileStore/st_checkpoint/autoloader/sfx_analytics/ga_visitors"
    # Creating a readStream to read the S3 data
    df = spark.readStream.format("cloudFiles") \
                        .option("cloudFiles.format", "csv") \
                        .option("sep", "\t") \
                        .option("cloudFiles.partitionColumns", "dbname") \
                        .option("cloudFiles.schemaEvolutionMode", "rescue") \
                        .option("cloudFiles.rescuedDataColumn", "rescued_data") \
                        .schema(schema) \
                        .load(input_path)

