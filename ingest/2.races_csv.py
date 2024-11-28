# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Ingest circuits csv file from Azure storage cliuster
# MAGIC
# MAGIC mount
# MAGIC Intgest
# MAGIC

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/azuredbstorageac/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

circuits_schema = StructType([
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True), 
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True, {"metadata": {"comment": "Latitude of the circuit"}}),
    StructField("lng", DoubleType(), True, {"metadata": {"comment": "Longitude of the circuit"}}),
    StructField("alt", IntegerType(), True, {"metadata": {"comment": "Altitude of the circuit"}}),
    StructField("url", StringType(), True),
    ])

# COMMAND ----------

circuits_df = spark.read.format("csv").option("header", "true").schema(circuits_schema).load("/mnt/azuredbstorageac/raw/circuits.csv")

# COMMAND ----------

from pyspark.sql.functions import col , lit, current_timestamp
from  datetime import datetime

# COMMAND ----------

df = circuits_df.select(col("circuitId"), circuits_df.name, "location",circuits_df["country"].alias("county")).show(10)

# COMMAND ----------

renamed_df =circuits_df.withColumnRenamed("circuitRef", "circuit_ref").withColumnRenamed("circuitId","circuit_id").withColumnRenamed("lat", "latitude").withColumnRenamed("lng", "longitude").withColumnRenamed("alt", "altitude").withColumn("date", current_timestamp()).withColumn("source", lit("circuits_csv"))


# COMMAND ----------

display(renamed_df)

# COMMAND ----------

type(renamed_df)
#display(renamed_df)

# COMMAND ----------



# COMMAND ----------

# write to processed folder
renamed_df.repartition(2).write.mode("overwrite").parquet("/mnt/azuredbstorageac/processed/circuits_data")

# COMMAND ----------

renamed_df.repartition(1).write.mode("overwrite").format("delta").save("/mnt/azuredbstorageac/processed/circuits_delta")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/azuredbstorageac/processed/circuits_delta/
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/azuredbstorageac/processed/circuits_data/

# COMMAND ----------

spark.read.parquet("/mnt/azuredbstorageac/processed/circuits_data").display()
