-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

show databases;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT
  *
FROM
  system.information_schema.tables
WHERE
  table_catalog = 'az_databrics_ws' and table_schema = 'demo'

-- COMMAND ----------

USE demo;

-- COMMAND ----------

CREATE TABLE test (id INT , name STRING);


-- COMMAND ----------

DESC TABLE EXTENDED test;

-- COMMAND ----------

show tables;

-- COMMAND ----------

DROP table demo.test;

-- COMMAND ----------

SHOW CREATE TABLE test;

-- COMMAND ----------

-- MAGIC %run "../includes/config/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(processed_folder_path)
-- MAGIC dbutils.fs.ls(f"{processed_folder_path}/results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.parquet(f"{processed_folder_path}/results")
-- MAGIC df.select("constructorId", "driverId", "position", "points").write.format("delta").mode("overwrite").save(f"{processed_folder_path}/results_ext_delta")
-- MAGIC #df.write.format("delta").mode("overwrite").saveAsTable("demo.results")
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df2 = spark.read.format("delta").load(f"{processed_folder_path}/results_ext_delta")
-- MAGIC df2.printSchema()

-- COMMAND ----------

show tables from demo;

-- COMMAND ----------

--SELECT * FROM demo.results;
DESCRIBE TABLE EXTENDED demo.results_ext;
--show CREATE TABLE demo.results;

-- COMMAND ----------

drop table if exists demo.results;

-- COMMAND ----------

DROP TABLE if exists demo.Results_Ext;

-- COMMAND ----------

create table IF NOT exists demo.Results_Ext(
  constructorId long, driverId long , position string, points double
  ) 
location 'abfss://processed@azuredbstorageac.dfs.core.windows.net/results_ext_delta'

-- COMMAND ----------

CREATE view demo.PERM_VIEW AS SELECT * FROM demo.Results_Ext where position <=10

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW  PERM_VIEW AS SELECT * FROM demo.Results_Ext where position <=5

-- COMMAND ----------

DESC EXTended PERM_VIEW;

-- COMMAND ----------

DESC TABLE EXTENDED  demo.Results_Ext;
