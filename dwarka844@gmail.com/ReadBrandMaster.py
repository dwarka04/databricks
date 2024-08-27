# Databricks notebook source
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
 
columns=["id","name","product_group_id","flavor_id"]
tableName="lscm_brand_master"

query = f"(SELECT {', '.join(columns)} FROM {tableName}) AS subquery"

# Load data from Google Cloud SQL into a DataFrame
df = spark.read.jdbc(url=jdbc_url, table=query, properties=db_properties)
 
# Show the DataFrame
df.show()

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("myshop."+tableName)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from myshop.lscm_brand_master;
# MAGIC
