# Databricks notebook source
# MAGIC %run ../../utils/util

# COMMAND ----------


env = "prod"
client = "clevelandclinic"
src_tb = "areaofinterest_candidate"
mySqlHost_prod = "jdbc:mysql://prod-xcloud-aurora-57-new.coc65bxwymxu.us-west-1.rds.amazonaws.com"


dbricksQuery = f"""select count(*) as row_count from prod.clevelandclinic.bronze_areaofinterest_candidate where modified_on < '2024-04-15 00:00:00'
"""
dbricksResult = spark.sql(dbricksQuery)
dbricksCount = dbricksResult.collect()[0]['row_count'] 
print(dbricksCount)
db_host = ""
db_username = "saurav.sagar"
db_password = "Purpl3M()useGl0wing"

mysql_df = spark.read \
            .format("jdbc") \
            .option("url", mySqlHost_prod) \
            .option("dbtable",f"{client}.{src_tb}") \
            .option("user", db_username) \
            .option("password", db_password) \
            .load()
mysql_df.createOrReplaceTempView("mysql_tempTable")

# Run SQL queries against the temporary view
mySqlQuery = f"""SELECT count(*) as row_count FROM mysql_tempTable where where modified_on < '2024-04-15 00:00:00'
"""
sqlResult = spark.sql(mySqlQuery)
mySqlCount = sqlResult.collect()[0]['row_count']
print(mySqlCount)

# comparequery = f"""
# """

# COMMAND ----------

import boto3
import pandas as pd
from io import StringIO

access_key = 'ASIA6D7HAOEDY2YAEAYC'
secret_key = 'MD05HydundUWSWV0YTC274gEGepWq/1vUDUldvme'
session = boto3.Session(access_key, secret_key)
s3 = session.client('s3')


bucket_name = 'hodes-cert-dih'
file_key = 'hodes-external-data/google-analytics4/archive-clean/ga_sources/dbname=aamc/ga_sources.20240226_003001.tsv.gz'

response = s3.list_buckets()
# response = s3.get_object(Bucket=bucket_name, Key=file_key)
# csv_content = response['Body'].read().decode('utf-8')
# df = pd.read_csv(StringIO(csv_content))

# COMMAND ----------

from pyspark.sql import SparkSession

client = "clevelandclinic"
trgfile = "ga_sources.20220720_233001.tsv.gz"

bucket_path = "s3://hodes-external-data/google-analytics/archive-clean/ga_sources/dbname=clevelandclinic/ga_sources.20220720_233001.tsv.gz"

p1 = "s3://hodes-external-data/google-analytics/archive-clean/ga_sources/"
p2 = "s3://hodes-external-data/google-analytics4/archive-clean/ga_sources/"

clientd = "dbname="+client+"/"
# files = dbutils.fs.ls(p1)
files = dbutils.fs.ls(p2)
count = 0
# parsing ga_source direct
for i in files:
    if(i[1]==clientd):
        print(i[0])
        srcf = dbutils.fs.ls(i[0])
        sumc = 0
        # parsing clientwise target files
        for j in srcf:
            # trg = dbutils.fs.ls(j[0])
            # li = spark.read.text(trg,lineSep="\n")
            # print(j[1])
            # if(j[1] == trgfile):
            li = spark.read.text(j[0],lineSep="\n")
            sumc = sumc + li.count()
        print(sumc)        
    count = count+1
print("number of client pr")




# lines = spark.read.text(bucket_path,lineSep="\n")
# lines.display()
# num_rows = lines.count()
# print(num_rows)

