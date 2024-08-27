# Databricks notebook source
# MAGIC %run ./BigQueryConnection

# COMMAND ----------

columns = ["id","Allotment_master_id","brand_id","packing_id","total_bottle"]

tableName = "lscm_allotment_detail" 

query = f"(SELECT {', '.join(columns)} FROM {tableName}) AS subquery"

df = spark.read.jdbc(
    url=jdbc_url,
    table=query,
    properties=db_properties
)

df.write.mode("overwrite").saveAsTable(f"myshop.{tableName}")

display(df)
