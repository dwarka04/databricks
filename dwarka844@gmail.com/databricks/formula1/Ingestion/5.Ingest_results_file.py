# Databricks notebook source
spark.read.json("/mnt/dwarka1/raw/2021-03-21/results.json").createOrReplaceTempView("results_cutover")

# COMMAND ----------

# MAGIC %sql
# MAGIC select raceId,count(1) from results_cutover group by raceId order by count(1) desc

# COMMAND ----------

spark.read.json("/mnt/dwarka1/raw/2021-04-18/results.json").createOrReplaceTempView("results_w2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select raceId,count(1) from results_w2 group by raceId order by count(1) desc

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ingest results.json file

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ###step 1  Read the json file using the spark datframe reader API

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")
v_file_date

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType, FloatType

# COMMAND ----------

results_schema =  StructType([StructField("resultId",IntegerType(),False),
StructField("raceId",IntegerType(),True),
StructField("driverId",IntegerType(),True),
StructField("constructorId",IntegerType(),True),
StructField("number",IntegerType(),True),
StructField("grid",IntegerType(),True),
StructField("position",IntegerType(),True),
StructField("positionText",StringType(),True),
StructField("positionOrder",IntegerType(),True),
StructField("points",FloatType(),True),
StructField("laps",IntegerType(),True),
StructField("time",StringType(),True),
StructField("milliseconds",IntegerType(),True),
StructField("fastestLap",IntegerType(),True),
StructField("rank",IntegerType(),True),
StructField("fastestLapTime",StringType(),True),
StructField("fastestLapSpeed",FloatType(),True),
StructField("statusId",StringType(),True)
])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ###step 2 rename columns and add new column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId","result_id") \
                                        .withColumnRenamed("raceId","race_id") \
                                        .withColumnRenamed("driverId","driver_id") \
                                        .withColumnRenamed("constructorId","constructor_id") \
                                        .withColumnRenamed("positionText","position_text") \
                                        .withColumnRenamed("positionOrder","position_order") \
                                        .withColumnRenamed("fastestLap","fastest_lap") \
                                        .withColumnRenamed("fastestLapTime","fastet_lap_time") \
                                        .withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
                                            .withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))\
.withColumn("ingestion_date",current_timestamp()) 

# COMMAND ----------

# MAGIC %md
# MAGIC ###step 3 Drop the unwanted column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###step 4 write to output to processed container in parquet format
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Method1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if spark.catalog.tableExists("f1_processed.results"):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION(race_id=#{race_id_list.race_id})")

# COMMAND ----------


# results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC Method2

# COMMAND ----------

overwrite_partition(results_final_df,'f1_processed','results','race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_processed.results;

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1) from f1_processed.results group by race_id order by race_id desc;

# COMMAND ----------

dbutils.notebook.exit("Success")
