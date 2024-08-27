# Databricks notebook source
# MAGIC %sql
# MAGIC create database f1_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC #####create circuits table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.circuits;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.circuits (
# MAGIC   circuitId INT,
# MAGIC   circuitRef STRING,
# MAGIC   name STRING,
# MAGIC   location STRING,
# MAGIC   country STRING,
# MAGIC   lat DOUBLE,
# MAGIC   lng DOUBLE,
# MAGIC   alt INT,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (path "/mnt/dwarka1/raw/circuits.csv", header "true", inferSchema "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_raw.circuits;

# COMMAND ----------

# MAGIC %md
# MAGIC ###### create races table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.races;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.races (
# MAGIC   raceId INT,
# MAGIC   year INT,
# MAGIC   round INT,
# MAGIC   circuitId INT,
# MAGIC   name STRING,
# MAGIC   date DATE,
# MAGIC   time STRING,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (path "/mnt/dwarka1/raw/races.csv", header "true", inferSchema "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_raw.races;

# COMMAND ----------

# MAGIC %md
# MAGIC #####create tables for json files

# COMMAND ----------

# MAGIC %md
# MAGIC **create Constructors table**
# MAGIC - single line json
# MAGIC - simple structure

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.constructors;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.constructors (
# MAGIC   constructorId INT,
# MAGIC   constructorRef STRING,
# MAGIC   
# MAGIC   name STRING,
# MAGIC   nationality STRING,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING json
# MAGIC OPTIONS (path "/mnt/dwarka1/raw/constructors.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_raw.constructors

# COMMAND ----------

# MAGIC %md
# MAGIC **create drivers table**
# MAGIC - single line json
# MAGIC - complex structure

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.drivers;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.drivers (
# MAGIC   driverId INT,
# MAGIC   driverRef STRING,
# MAGIC   number INT,
# MAGIC   code STRING,
# MAGIC   name STRUCT<forename: STRING, surname: STRING>,
# MAGIC   dob DATE,
# MAGIC   nationality STRING,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING json
# MAGIC OPTIONS (path "/mnt/dwarka1/raw/drivers.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_raw.drivers

# COMMAND ----------

# MAGIC %md
# MAGIC **create results table**
# MAGIC - single line json
# MAGIC - simple structure

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.results;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.results (
# MAGIC   resultId INT,
# MAGIC   raceId INT,
# MAGIC   driverId INT,
# MAGIC   constructorId INT,
# MAGIC   number INT,
# MAGIC   position INT,
# MAGIC   positionText STRING,
# MAGIC   points INT,
# MAGIC   laps INT,
# MAGIC   time STRING,
# MAGIC   milliseconds INT,
# MAGIC   fastestLap STRING,
# MAGIC   rank INT,
# MAGIC   fastestLapTime STRING,
# MAGIC   fastestLapSpeed FLOAT,
# MAGIC   statusId INT
# MAGIC )
# MAGIC USING json
# MAGIC OPTIONS (path "/mnt/dwarka1/raw/results.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_raw.results

# COMMAND ----------

# MAGIC %md
# MAGIC **create pit_stops table**
# MAGIC - multi line json
# MAGIC - simple structure

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.pit_stops;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.pit_stops (
# MAGIC   driverId INT,
# MAGIC   duration STRING,
# MAGIC   
# MAGIC   lap INT,
# MAGIC   milliseconds INT,
# MAGIC   raceId INT,
# MAGIC   stop INT,
# MAGIC   time STRING
# MAGIC )
# MAGIC USING json
# MAGIC OPTIONS (path "/mnt/dwarka1/raw/pit_stops.json",multiline "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_raw.pit_stops

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create tables for list of files
# MAGIC **create LAP Times table**
# MAGIC - csv file
# MAGIC - multiple files

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.lap_times;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.lap_times (
# MAGIC   raceId INT,
# MAGIC   driverId INT,
# MAGIC   
# MAGIC   lap INT,
# MAGIC   position INT,
# MAGIC   time STRING,
# MAGIC   milliseconds INT
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (path "/mnt/dwarka1/raw/lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_raw.lap_times

# COMMAND ----------

# MAGIC %md
# MAGIC **create Qualifying table**
# MAGIC - json file
# MAGIC - multiplline json
# MAGIC - multiple files

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.qualifying;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.qualifying (
# MAGIC   constructorId INT,
# MAGIC   driverId INT,
# MAGIC   number INT,
# MAGIC   position INT,
# MAGIC   q1 STRING,
# MAGIC   q2 STRING,
# MAGIC   q3 STRING,
# MAGIC   qualifyId INT,
# MAGIC   raceId INT
# MAGIC   
# MAGIC )
# MAGIC USING json
# MAGIC OPTIONS (path "/mnt/dwarka1/raw/qualifying",multiline "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_raw.qualifying
