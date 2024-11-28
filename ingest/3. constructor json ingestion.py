# Databricks notebook source
display(dbutils.fs.ls("/mnt/azuredbstorageac/raw"))

# COMMAND ----------

df = spark.read.json("/mnt/azuredbstorageac/raw/constructors.json")
df.printSchema()
display(df)

# COMMAND ----------

schema = "constructorId INT,constructorRef STRING,  name STRING, nationality STRING, url STRING"

# COMMAND ----------

source_df = spark.read.format("json").schema(schema).load("/mnt/azuredbstorageac/raw/drivers.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_df = source_df.withColumnRenamed("constructorId","constructor_id").withColumnRenamed("constructorRef","constructor_ref").withColumn("source",lit("constructors.json")).withColumn("load_date",current_timestamp())

# COMMAND ----------

final_df.write.format("delta").mode("overwrite").save("/mnt/azuredbstorageac/processed/constructors")
