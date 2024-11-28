# Databricks notebook source
display(dbutils.fs.ls("/mnt/azuredbstorageac/raw"))

# COMMAND ----------

df = spark.read.json("/mnt/azuredbstorageac/raw/drivers.json")

from pyspark.sql.functions import explode, col, lit , concat, current_timestamp

df = df.withColumn("name",concat(col("name.forename"), lit(" "), col("name.surname")).alias("full_name")).withColumn("source",lit("drivers")).withColumn("ingestion_dt",current_timestamp())
df.printSchema()
df.show(2)

# COMMAND ----------

df.write.mode("overwrite").format("parquet").save("/mnt/azuredbstorageac/processed/results")

# COMMAND ----------

spark.read.option("multiLine", True).json("/mnt/azuredbstorageac/raw/pit_stops.json").display()

# COMMAND ----------

spark.read.option("multiLine", True).json("/mnt/azuredbstorageac/raw/qualifying/").display()

# COMMAND ----------

df = spark.read.json("/mnt/azuredbstorageac/raw/results.json")

from pyspark.sql.functions import explode, col, lit , concat, current_timestamp

df = df.withColumn("source",lit("results")).withColumn("ingestion_dt",current_timestamp())
df.printSchema()
display(df)
df.write.mode("overwrite").saveAsTable("raw.results")
