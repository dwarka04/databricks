# Databricks notebook source
# Ensure %run command is in a cell by itself
%run ./BigQueryConnection

columns = ["id", "name"]
tableName = "lscm_flavor_master"

query = f"(SELECT {', '.join(columns)} FROM {tableName}) AS subquery"

# Load data from Google Cloud SQL into a DataFrame
df = spark.read.jdbc(
    url=jdbc_url,
    table=query,
    properties=db_properties
)

df.write.mode("overwrite").saveAsTable(f"myshop.{tableName}")

# Show the DataFrame
display(df)
