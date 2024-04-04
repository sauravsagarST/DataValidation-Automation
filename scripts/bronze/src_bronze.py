# Databricks notebook source
env = "cert"
scope = f"{env}-credentials"
db_username = dbutils.secrets.get(scope=f"{scope}", key="db-user")
db_password = dbutils.secrets.get(scope=f"{scope}", key="db-password")

print(db_username)
print(db_password)