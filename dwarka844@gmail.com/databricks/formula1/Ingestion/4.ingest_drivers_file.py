# Databricks notebook source
# MAGIC %md
# MAGIC Ingest driver.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ###step 1 read the json file using the spart dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

name_schema = StructType([StructField("forename",StringType(),True),
StructField("surname",StringType(),True)
])

# COMMAND ----------

drivers_schema =  StructType([StructField("driverId",IntegerType(),True),
StructField("driverRef",StringType(),True),
StructField("number",IntegerType(),True),
StructField("code",StringType(),True),
StructField("name",name_schema),
StructField("dob",DateType(),True),
StructField("nationality",StringType(),True),
StructField("url",StringType(),True),
])

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")


# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###step 2 rename columns and add new column
# MAGIC 1 driverId renamed to driver_id
# MAGIC 2 driverRef renamed to driver_ref
# MAGIC 3 ingestion date added
# MAGIC 4 name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import  col,concat,current_timestamp,lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("driverRef","driver_ref") \
.withColumn("ingestion_date",current_timestamp()) \
.withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname"))) \
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(drivers_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###step 3 - drop the unwanted column
# MAGIC 1 name.forename
# MAGIC 2 name.surname 
# MAGIC 3 url

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###wite output to parquet file

# COMMAND ----------


drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.drivers

# COMMAND ----------

dbutils.notebook.exit("Success")
