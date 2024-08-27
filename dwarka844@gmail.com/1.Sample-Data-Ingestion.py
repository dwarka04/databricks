# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS myshop2

# COMMAND ----------

# Define the connection parameters for the Google Cloud SQL database
jdbc_url = "jdbc:mysql://34.38.210.233:3306/myshop"  # For MySQL
# jdbc_url = "jdbc:postgresql://34.38.210.233:5432/myshop"  # For PostgreSQL

# Database credentials
db_properties = {
    "user": "retailpos",
    "password": "retailpos",
    "driver": "com.mysql.cj.jdbc.Driver"  # For MySQL
    # "driver": "org.postgresql.Driver"  # For PostgreSQL
}

try:
    existing_df = spark.table(myshop2.lscm_brand_master)
    table_exists = True
except Exception:
    table_exists = False

if table_exists:
    # Read the existing data from Databricks table
    existing_df = spark.table("myshop2.lscm_brand_master")

    # Load data from Google Cloud SQL into a DataFrame
    new_df = spark.read.jdbc(url=jdbc_url, table="lscm_brand_master", properties=db_properties)

    # Perform a left anti join to find records that are in new_df but not in existing_df
    # Make sure to use appropriate column(s) to check for uniqueness
    unique_new_df = new_df.alias("new").join(
        existing_df.alias("existing"),
        on=["ID"],  # Replace with the column(s) that uniquely identify rows
        how="left_anti"
    )

    # Union the existing data with the unique new data
    combined_df = existing_df.unionByName(unique_new_df)

    # Write the combined DataFrame back to the Databricks table
    combined_df.write.mode("overwrite").saveAsTable("myshop2.lscm_brand_master")

    # Show the DataFrame to confirm
    combined_df.display()


else:
    new_df.write.mode("overwrite").saveAsTable("myshop2.lscm_brand_master")

    # Show the DataFrame to confirm
    new_df.display()

