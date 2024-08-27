# Databricks notebook source
# MAGIC %md
# MAGIC ###Access dataframes using sql
# MAGIC #####objectives
# MAGIC #######1 .Create Temprory view on dataframes
# MAGIC #######2 .Access the view from sql cell
# MAGIC #######3 .Access the view from python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from v_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

p_race_year =2019

# COMMAND ----------

race_results_2019_df = spark.sql(f"select * from v_race_results where race_year ={p_race_year}")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Global Temp views
# MAGIC #####objectives
# MAGIC #######1 .Create GLobal Temprory view on dataframes
# MAGIC #######2 .Access the view from sql cell
# MAGIC #######3 .Access the view from python cell
# MAGIC #######4 .Access the view from another notebook

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results;

# COMMAND ----------

spark.sql("select * from global_temp.gv_race_results").show()
