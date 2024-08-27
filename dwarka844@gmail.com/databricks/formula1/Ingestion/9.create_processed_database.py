# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE if not exists f1_processed
# MAGIC LOCATION '/mnt/dwarka1/processed';

# COMMAND ----------

# MAGIC %sql
# MAGIC desc database f1_raw
