# Databricks notebook source
# MAGIC %run ./BigQueryConnection

# COMMAND ----------

columns = ["id","terminal_id","invoice_no","invoice_date","brand_id","packing_id","qty","rate_per_bottle","tax_percent","total_amt","invoice_amt","issynced","syncedOn","payment_mode","is_carry_bag","online_order_ref_no","Fin_year_auto","cess_per_qty"]

tableName = "retail_invoice"

query = f"(SELECT {', '.join(columns)} FROM {tableName}) AS subquery"

df = spark.read.jdbc(
    url=jdbc_url,
    table=query,
    properties=db_properties
)

df.write.mode("overwrite").saveAsTable(f"myshop.{tableName}")

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from myshop.retail_invoice;
