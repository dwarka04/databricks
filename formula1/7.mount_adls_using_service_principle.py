# Databricks notebook source
# MAGIC %md 
# MAGIC MOunt azuredata lake using service principle
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

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenent_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@dwarka1.dfs.core.windows.net/",
  mount_point = "/mnt/dwarka1/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/dwarka1/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/dwarka1/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/dwarka1/demo')
