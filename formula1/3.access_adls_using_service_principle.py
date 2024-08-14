# Databricks notebook source
# MAGIC %md 
# MAGIC Access Azure data lake using service principle
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope',key = 'formula1-app-client-id')
tenent_id = dbutils.secrets.get(scope = 'formula1-scope',key = 'formula1-app-tenent-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope',key = 'formula1-app-client-secret')

# COMMAND ----------



spark.conf.set("fs.azure.account.auth.type.dwarka1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.dwarka1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dwarka1.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.dwarka1.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.dwarka1.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenent_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dwarka1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dwarka1.dfs.core.windows.net/circuits.csv"))
