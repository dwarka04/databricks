# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.dwarka1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.dwarka1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.dwarka1.dfs.core.windows.net", "sp=rl&st=2024-08-14T20:14:28Z&se=2024-08-15T04:14:28Z&spr=https&sv=2022-11-02&sr=c&sig=G0W2G6%2BqsPrE8q%2BRDz4fJZ6ZXNsrL47aIrV%2B29LDpyk%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dwarka1.dfs.core.windows.net"))

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dwarka1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dwarka1.dfs.core.windows.net/circuits.csv"))
