# Databricks notebook source
# MAGIC %md 
# MAGIC MOunt azuredata lake container for the project
# MAGIC

# COMMAND ----------

def mount_adls(storage_account_name,container_name):
    client_id = dbutils.secrets.get(scope = 'formula1-scope',key = 'formula1-app-client-id')
    tenent_id = dbutils.secrets.get(scope = 'formula1-scope',key = 'formula1-app-tenent-id')
    client_secret = dbutils.secrets.get(scope = 'formula1-scope',key = 'formula1-app-client-secret')
    spark.conf.set("fs.azure.account.auth.type.dwarka1.dfs.core.windows.net", "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type.dwarka1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id.dwarka1.dfs.core.windows.net", client_id)
    spark.conf.set("fs.azure.account.oauth2.client.secret.dwarka1.dfs.core.windows.net", client_secret)
    spark.conf.set("fs.azure.account.oauth2.client.endpoint.dwarka1.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenent_id}/oauth2/token")
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenent_id}/oauth2/token"}
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
            dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC Mount Raw container

# COMMAND ----------

mount_adls('dwarka1','raw')

# COMMAND ----------

mount_adls('dwarka1','presentation')

# COMMAND ----------

mount_adls('dwarka1','processed')

# COMMAND ----------

dbutils.fs.ls("/mnt/dwarka1/raw/")
