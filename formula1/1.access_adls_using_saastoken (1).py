# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.dwarka1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.dwarka1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.dwarka1.dfs.core.windows.net", "sp=rl&st=2024-07-30T12:36:34Z&se=2024-07-30T20:36:34Z&spr=https&sv=2022-11-02&sr=c&sig=miaTNH%2BPQbttGohoFjDGFbS5XAMUt7OQIsn3I1jF8Yo%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dwarka1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dwarka1.dfs.core.windows.net/circuits.csv"))
