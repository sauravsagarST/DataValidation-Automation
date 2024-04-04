# Databricks notebook source
# MAGIC %run ../scripts/bronze/bronze_dict

# COMMAND ----------

from datetime import datetime

def executionTime():
    exec_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return exec_time

def testId():
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