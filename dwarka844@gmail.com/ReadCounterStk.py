# Databricks notebook source
# MAGIC %run ./BigQueryConnection

# COMMAND ----------

columns = ["id","FIN_Year","Brand_id","Packing_id","QTY_Opening","Qty_Inward","Qty_Outward","Qty_Broken","QTy_balance","counter_id"]

tableName = "lscm_counterstk_summ" 

query = f"(SELECT {', '.join(columns)} FROM {tableName}) AS subquery"

df = spark.read.jdbc(
    url=jdbc_url,
    table=query,
    properties=db_properties
)

df.write.mode("overwrite").saveAsTable(f"myshop.{tableName}")

display(df)
