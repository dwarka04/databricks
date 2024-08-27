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