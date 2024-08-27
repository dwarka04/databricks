# Databricks notebook source
# MAGIC %sql
# MAGIC use f1_processed;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_presentation.calculated_race_results
# MAGIC    USING parquet
# MAGIC    AS
# MAGIC    SELECT races.race_year,
# MAGIC           constructors.name AS team_name,
# MAGIC           drivers.name AS driver_name,
# MAGIC           results.position,
# MAGIC           results.points,
# MAGIC           11 - results.position AS calculated_points
# MAGIC    FROM hive_metastore.f1_processed.results
# MAGIC    JOIN hive_metastore.f1_processed.drivers ON (results.driver_id = drivers.driver_id)
# MAGIC    JOIN hive_metastore.f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
# MAGIC    JOIN hive_metastore.f1_processed.races ON (results.race_id = races.race_id)
# MAGIC    WHERE results.position <= 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.calculated_race_results
