# Databricks notebook source
# MAGIC %run ./BigQueryConnection

# COMMAND ----------

columns = ["id","FIN_Year","Brand_id","Packing_id","QTY_Opening","Qty_Inward","Qty_Outward","Qty_Broken","QTy_balance","qty_dead_reported"]

tableName = "lscm_stk_summ" 

query = f"(SELECT {', '.join(columns)} FROM {tableName}) AS subquery"

df = spark.read.jdbc(
    url=jdbc_url,
    table=query,
    properties=db_properties
)

df.write.mode("overwrite").saveAsTable(f"myshop.{tableName}")

display(df)

# COMMAND ----------

import urllib.request
response = urllib.request.urlopen('http://ifconfig.me/ip')
external_ip = response.read().decode('utf-8')
print("External IP Address: ", external_ip)

# COMMAND ----------

columns = [
    "id", "FIN_Year", "Brand_id", "Packing_id", "QTY_Opening", 
    "Qty_Inward", "Qty_Outward", "Qty_Broken", "QTy_balance", "qty_dead_reported"
]

tableName = "lscm_stk_summ"

query = f"(SELECT {', '.join(columns)} FROM {tableName}) AS subquery"
# Define the connection parameters
jdbc_url = "jdbc:mysql://<PUBLIC_IP>:<PORT>/<DATABASE>"  # For MySQL
#jdbc_url = "jdbc:postgresql://<PUBLIC_IP>:<PORT>/<DATABASE>"  # For PostgreSQL
 
# Replace the placeholders with your actual values
jdbc_url = jdbc_url.replace("<PUBLIC_IP>", "34.38.210.233")
jdbc_url = jdbc_url.replace("<PORT>", "3306")  # Default MySQL port; use 5432 for PostgreSQL
jdbc_url = jdbc_url.replace("<DATABASE>", "myshop")
 
# Database credentials
db_properties = {
    "user": "retailpos",
    "password": "retailpos",
    "driver": "com.mysql.cj.jdbc.Driver"  # For MySQL
    # "driver": "org.postgresql.Driver"  # For PostgreSQL
}
df = spark.read.jdbc(
    url=jdbc_url,
    table=query,
    properties=db_properties
)

df.write.mode("overwrite").saveAsTable(f"myshop.{tableName}")

display(df)

# COMMAND ----------

# Cell 2
# Define the connection parameters
jdbc_url = "jdbc:mysql://<PUBLIC_IP>:<PORT>/<DATABASE>"  # For MySQL
#jdbc_url = "jdbc:postgresql://<PUBLIC_IP>:<PORT>/<DATABASE>"  # For PostgreSQL

# Replace the placeholders with your actual values
jdbc_url = jdbc_url.replace("<PUBLIC_IP>", "34.38.210.233")
jdbc_url = jdbc_url.replace("<PORT>", "3306")  # Default MySQL port; use 5432 for PostgreSQL
jdbc_url = jdbc_url.replace("<DATABASE>", "myshop")

# Database credentials
db_properties = {
    "user": "retailpos",
    "password": "retailpos",
    "driver": "com.mysql.cj.jdbc.Driver"  # For MySQL
    # "driver": "org.postgresql.Driver"  # For PostgreSQL
}

# COMMAND ----------

# Cell 3
columns = [
    "id", "FIN_Year", "Brand_id", "Packing_id", "QTY_Opening", 
    "Qty_Inward", "Qty_Outward", "Qty_Broken", "QTy_balance", "qty_dead_reported"
]

tableName = "lscm_stk_summ"

query = f"(SELECT {', '.join(columns)} FROM {tableName}) AS subquery"

df = spark.read.jdbc(
    url=jdbc_url,
    table=query,
    properties=db_properties
)

df.write.mode("overwrite").saveAsTable(f"myshop.{tableName}")

display(df)

# COMMAND ----------


