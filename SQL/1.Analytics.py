# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM RAW.driver limit 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM RAW.race limit 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE PROCESSED.RESULTS USING parquet LOCATION 'abfss://processed@azuredbstorageac.dfs.core.windows.net/processed/results'
# MAGIC AS 
# MAGIC SELECT 
# MAGIC d.driverId, d.name, d.nationality,  p.position, p.points
# MAGIC FROM RAW.DRIVER d
# MAGIC JOIN 
# MAGIC RAW.RESULTS p ON p.driverId = d.driverId
# MAGIC WHERE p.position <11; 

# COMMAND ----------

# MAGIC %sql DESC table extended PROCESSED.RESULTS ;
