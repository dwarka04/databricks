# Databricks notebook source
# MAGIC %md 
# MAGIC #####Objective
# MAGIC spark sql documentation
# MAGIC create db demo
# MAGIC data tab in ui
# MAGIC show command
# MAGIC describe comman
# MAGIC show current db

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW databases

# COMMAND ----------

# MAGIC %sql
# MAGIC describe database demo

# COMMAND ----------

# MAGIC %sql
# MAGIC describe database  extended demo

# COMMAND ----------

# MAGIC %sql
# MAGIC select  current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC use demo;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

# MAGIC %md 
# MAGIC **objective
# MAGIC create manage table using phython
# MAGIC create manage table using sql
# MAGIC effect of dropping a manged table
# MAGIC describe table**

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df =  spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------


race_results_df.write.format("parquet").saveAsTable("demo.race_results_phyton")


# COMMAND ----------

# MAGIC %sql
# MAGIC use demo;
# MAGIC show tables

# COMMAND ----------

race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

# COMMAND ----------


