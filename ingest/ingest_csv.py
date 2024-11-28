# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Ingest circuits csv file from Azure storage cliuster
# MAGIC
# MAGIC mount
# MAGIC Intgest
# MAGIC

# COMMAND ----------

#display(dbutils.fs.mounts())

# COMMAND ----------

#%fs
#ls /mnt/azuredbstorageac/raw

# COMMAND ----------

df = spark.read.csv("/mnt/azuredbstorageac/raw/races.csv", header="true", inferSchema="true")
#display(df)

# COMMAND ----------

#df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType 

race_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])
circuits_df = spark.read.format("csv").option("header", "true").schema(race_schema).load("/mnt/azuredbstorageac/raw/races.csv")

# COMMAND ----------

from pyspark.sql.functions import col , lit, current_timestamp
from  datetime import datetime

# COMMAND ----------

#df = circuits_df.select(col("raceId"), circuits_df.date, "time",circuits_df["name"].alias("county")).show(10)

# COMMAND ----------

renamed_df =circuits_df.withColumnRenamed("raceId", "race_id").withColumnRenamed("circuitId","circuit_id").drop("url")


# COMMAND ----------

# MAGIC %run "../includes/utilities"

# COMMAND ----------

from pyspark.sql.functions import concat,to_timestamp
renamed_df = renamed_df.withColumn("race_timestamp", to_timestamp(concat(col("date"),lit(' '),col("time")), 'yyyy-MM-dd HH:mm:ss'))
renamed_df = audit_col(renamed_df,"races_csv")
#.withColumn("ingestion_dt",current_timestamp()).withColumn("source", lit("races_csv"))

# COMMAND ----------

#display(renamed_df)

# COMMAND ----------

#type(renamed_df)
#display(renamed_df)

# COMMAND ----------

# write to processed folder
renamed_df.repartition(1).write.mode("overwrite").parquet("/mnt/azuredbstorageac/processed/races_data")

# COMMAND ----------

#renamed_df.repartition(2).write.mode("overwrite").format("delta").save("/mnt/azuredbstorageac/processed/races_delta")

# COMMAND ----------

spark.read.parquet("/mnt/azuredbstorageac/processed/races_data/").display()

# COMMAND ----------

#%fs
#ls /mnt/azuredbstorageac/processed/races_data/



# COMMAND ----------

#spark.read.format("delta").load("/mnt/azuredbstorageac/processed/races_delta/").display()

# COMMAND ----------

#renamed_df.write.partitionBy("year").format("parquet").mode("overwrite").save("/mnt/azuredbstorageac/processed/races_data_renamed/")
