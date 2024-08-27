# Databricks notebook source
# MAGIC %run ./BigQueryConnection

# COMMAND ----------

columns = ["id", "name"]
tableName = "lscm_product_group_master"

query = f"(SELECT {', '.join(columns)} FROM {tableName}) AS subquery"

df = spark.read.jdbc(
    url=jdbc_url,
    table=query,
    properties=db_properties
)

df.write.mode("overwrite").saveAsTable(f"myshop.{tableName}")

display(df)
