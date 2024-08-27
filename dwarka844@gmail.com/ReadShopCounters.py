# Databricks notebook source
# MAGIC %run ./BigQueryConnection

# COMMAND ----------

columns = ["id","`Desc`","Short_name","is_online_counter"]

tableName = "lscm_shop_counter" 

query = f"(SELECT {', '.join(columns)} FROM {tableName}) AS subquery"

df = spark.read.jdbc(
    url=jdbc_url,
    table=query,
    properties=db_properties
)

df.write.mode("overwrite").saveAsTable(f"myshop.{tableName}")

display(df)
