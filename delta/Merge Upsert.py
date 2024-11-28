# Databricks notebook source
# MAGIC %run "../includes/config/configuration"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT  distinct year from demo.race2;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO demo.race1 as target
# MAGIC USING demo.race2 as source
# MAGIC ON source.year = target.year and source.raceId = target.raceId
# MAGIC -- WHEN MATCHED THEN
# MAGIC --   UPDATE SET
# MAGIC --     id = people10mupdates.id,
# MAGIC --     firstName = people10mupdates.firstName,
# MAGIC --     middleName = people10mupdates.middleName,
# MAGIC --     lastName = people10mupdates.lastName,
# MAGIC --     gender = people10mupdates.gender,
# MAGIC --     birthDate = people10mupdates.birthDate,
# MAGIC --     ssn = people10mupdates.ssn,
# MAGIC --     salary = people10mupdates.salary
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     raceId,
# MAGIC year,
# MAGIC round,
# MAGIC circuitId,
# MAGIC name,
# MAGIC date,
# MAGIC time
# MAGIC   )
# MAGIC   VALUES (
# MAGIC      source.raceId,
# MAGIC source.year,
# MAGIC source.round,
# MAGIC source.circuitId,
# MAGIC source.name,
# MAGIC source.date,
# MAGIC source.time
# MAGIC   )

# COMMAND ----------

from delta.tables import *

deltaTablePeople = DeltaTable.forPath(spark, '/tmp/delta/people-10m')
deltaTablePeopleUpdates = DeltaTable.forPath(spark, '/tmp/delta/people-10m-updates')

dfUpdates = deltaTablePeopleUpdates.toDF()

deltaTablePeople.alias('people') \
  .merge(
    dfUpdates.alias('updates'),
    'people.id = updates.id'
  ) \
  .whenMatchedUpdate(set =
    {
      "id": "updates.id",
      "firstName": "updates.firstName",
      "middleName": "updates.middleName",
      "lastName": "updates.lastName",
      "gender": "updates.gender",
      "birthDate": "updates.birthDate",
      "ssn": "updates.ssn",
      "salary": "updates.salary"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "id": "updates.id",
      "firstName": "updates.firstName",
      "middleName": "updates.middleName",
      "lastName": "updates.lastName",
      "gender": "updates.gender",
      "birthDate": "updates.birthDate",
      "ssn": "updates.ssn",
      "salary": "updates.salary"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %md History, Timetravel and Vaccume
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESC history demo.race1 
# MAGIC

# COMMAND ----------

display(spark.sql("DESC HISTORY demo.race1 "))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.race1 version as of 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table EXTENDED demo.race_part

# COMMAND ----------

#df = spark.read.option("versionAsOf", 0).table("demo.race1")
#df = spark.read.option("versionAsOf", 0).load("abfss://raw@azuredbstorageac.dfs.core.windows.net/race_part")
df = spark.read.option("versionAsOf", 0).load(f"{raw_folder_path}/race_part")

# COMMAND ----------

display(df)

# COMMAND ----------

display(spark.sql("SELECT * FROM demo.race1 version as of 0 "))

# COMMAND ----------

# MAGIC %md
# MAGIC Vaccume
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC SET spark.databricks.delta.vacuum.parallelDelete.enabled = true;

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM demo.race1 

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history demo.race1 

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM demo.race1 RETAIN 0 HOURS 
# MAGIC --DRY RUN

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history demo.race1 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo.race1 version as of 2 

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM demo.race1  where raceId >1050

# COMMAND ----------

# MAGIC %sql 
# MAGIC INSERT INTO demo.race1 select * from demo.race1  VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql Desc history  demo.race1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM demo.race1 VERSION AS OF 12

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO demo.race1 tgt
# MAGIC USING  demo.race1 VERSION AS OF 1 src
# MAGIC ON tgt.raceId = src.raceId
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from demo.race1

# COMMAND ----------

display( spark.sql("SELECT count(*) FROM demo.race1 "))
# remove duplicates;
df = spark.sql("SELECT * FROM demo.race1 ")
df_de_dup = df.dropDuplicates()
df_de_dup = df.dropDuplicates(['raceId'])



