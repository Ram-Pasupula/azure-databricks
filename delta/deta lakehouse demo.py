# Databricks notebook source
# MAGIC %run "../includes/config/configuration/"

# COMMAND ----------

# MAGIC %run "../includes/utilities"
# MAGIC
# MAGIC

# COMMAND ----------

mount_adls("azuredbstorageac", "demo")

# COMMAND ----------

raw_folder_path
demo_folder_path

# COMMAND ----------

schema = "raceId Integer, year Integer, round Integer, circuitId Integer, name String, date String, time String, url String"


# COMMAND ----------

df = spark.read.format("csv").option("header", "true").schema(schema).option("delimiter", ",").option("mode", "FAILFAST").load(f"{raw_folder_path}/races.csv")

df.filter("year > 2018").write.mode("overwrite").format("delta").saveAsTable("demo.race2")

 #df.filter("year > 2018").write.mode("overwrite").format("delta").partitionBy("raceId").save(f"{raw_folder_path}/race_part")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS demo.race_part using delta options (path "abfss://raw@azuredbstorageac.dfs.core.windows.net/race_part");

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS demo.race_part
# MAGIC CREATE TABLE IF NOT EXISTS demo.races1 AS SELECT * FROM demo.race_part WHERE year > 2018

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists demo.races
# MAGIC USING delta
# MAGIC location 'abfss://processed@azuredbstorageac.dfs.core.windows.net/races'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   read_files(
# MAGIC     '/mnt/azuredbstorageac/raw/races.csv',
# MAGIC     header => "True",
# MAGIC     sep => ","
# MAGIC   )
# MAGIC limit 2
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM CSV.`/mnt/azuredbstorageac/raw/races.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into demo.table1
# MAGIC   --from "dbfs:/FileStore/tables/mycsv2.csv"
# MAGIC   from '/mnt/azuredbstorageac/raw/races.csv'
# MAGIC   FILEFORMAT = csv
# MAGIC   FORMAT_OPTIONS('header'='true','inferSchema'='True');

# COMMAND ----------

# MAGIC %sql DESCRIBE EXTENDED demo.racesss

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo.racesss limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE demo.racesss SET name = 'INDIA GRAND Prix' WHERE name = "Chinese Grand Prix"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo.racesss VERSION AS OF 2 WHERE name like 'Chin%' ;
# MAGIC --DESCRIBE HISTORY demo.racesss

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forName(spark, "demo.racesss")

Declare the predicate by using a SQL-formatted string.
deltaTable.update(
  condition = "name = 'INDIA GRAND Prix'",
  set = { "name": "'Chinese Grand Prix'" }
)

# Declare the predicate by using Spark SQL functions.
# deltaTable.update(
#   condition = col('gender') == 'M',
#   set = { 'gender': lit('Male') }
# )

deltaTable_path = DeltaTable.forPath(spark, f"{raw_folder_path}/racesss")
deltaTable_path.update(
    condition="time = '07:00:00'",
    set = { "time": "'06:00:00'" }
)


# COMMAND ----------

#delete from table


# Declare the predicate by using a SQL-formatted string.
#deltaTable.delete("time = '"\\N"'")

# Declare the predicate by using Spark SQL functions.
deltaTable.delete(col('time') == lit('\\N'))

# COMMAND ----------

# MAGIC %md
# MAGIC Query an earlier version of the table (time travel)
# MAGIC
# MAGIC

# COMMAND ----------

#display(deltaTable.history())

deltaHistory = deltaTable.history()
display(deltaHistory.where("version == 0"))

df = spark.read.option('versionAsOf', 0).table("demo.racesss")
# Or:
#df = spark.read.option('timestampAsOf', '2024-05-15T22:43:15.000+00:00').table("main.default.people_10m")

#display(df)
# Or:
#display(deltaTable.where("timestamp == '2024-05-15T22:43:15.000+00:00'"))

# COMMAND ----------

deltaHistory = deltaTable.history()

display(deltaHistory.where("version == 0"))
# Or:
#display(deltaHistory.where("timestamp == '2024-05-15T22:43:15.000+00:00'"))

# COMMAND ----------


