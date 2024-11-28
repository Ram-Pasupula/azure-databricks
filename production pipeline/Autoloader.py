# Databricks notebook source
# MAGIC %run "../includes/config/configuration"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --DROP TABLE IF EXISTS demo.copyinto;
# MAGIC --CREATE TABLE IF NOT EXISTS demo.copyinto;
# MAGIC
# MAGIC COPY INTO demo.copyinto
# MAGIC FROM '/Volumes/az_databrics_ws/processed/laps'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
# MAGIC COPY_OPTIONS ('overwrite' = 'false', 'schemaLocation' = 'dbfs:/Volumes/az_databrics_ws/processed/circuit', 'mergeSchema' = 'true');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM demo.copyinto;

# COMMAND ----------

# MAGIC %md Autoloader

# COMMAND ----------

checkpoint_path = "dbfs:/Volumes/az_databrics_ws/processed/autoloader"

(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load("/Volumes/az_databrics_ws/processed/laps")
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  #.trigger(availableNow=True)
  .toTable("prod.al_circuits"))


# COMMAND ----------

spark.readStream.table("prod.al_circuits").isStreaming

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from prod.al_circuits;
