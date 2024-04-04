# Databricks notebook source
bronze_candidate_dict = {
    'duplicate_tc': ['candidate_id','users_id'],
    'null_tc': ['candidate_id','users_id','uname']
}
bronze_folder_dict = {
    'duplicate_tc': ['folder_id'],
    'null_tc': ['folder_id','users_id','hierarchy_id']
}

