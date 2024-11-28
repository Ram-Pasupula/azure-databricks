# Databricks notebook source
# MAGIC %run "../includes/config/configuration"

# COMMAND ----------


display(dbutils.fs.ls(processed_folder_path) )

# COMMAND ----------

race_df = spark.read.format("parquet").load(f"{processed_folder_path}/races_data/").select("race_id", "circuit_id", "year").filter("year>=2018")
race_df.show(10)

# COMMAND ----------

results_df = spark.read.format("parquet").load(f"{processed_folder_path}/results/")

    

# COMMAND ----------

race_results_df = results_df.join(race_df, race_df.race_id == results_df.raceId, "inner")

display(race_results_df)

# COMMAND ----------

driver_df = spark.read.format("parquet").load(f"{processed_folder_path}/drivers/")

# COMMAND ----------


from pyspark.sql.functions import col, length
#driver_df.drop("ingestion_dt","source","number").filter(~col("code").contains("\\N")).join(results_df, "driverId", "semi").write.mode("overwrite").format("parquet").save(f"{processed_folder_path}/results_with_drivers/")

driver_df.drop("ingestion_dt","source","number").filter(length(col("code")) > 2).join(race_results_df, "driverId", "inner").write.mode("overwrite").format("delta").save(f"{processed_folder_path}/results_with_drivers_delta/")


# COMMAND ----------

df = spark.read.format("delta").load(f"{processed_folder_path}/results_with_drivers_delta/")
df.write.saveAsTable("results")

# COMMAND ----------

# Group by drive and get total poiints
from pyspark.sql.functions import col, sum, count, countDistinct

df = spark.read.parquet(f"{processed_folder_path}/results_with_drivers/")
df.cache()
df.createOrReplaceTempView("results")

# display(df.groupBy("driverId").agg(sum("points").alias("total_points"),count("points").alias("total_races")).orderBy(sum("points").desc()).filter(col("total_races") > 10).filter(col("total_points") > 100))
#display(spark.sql("select * from results limit 5"))
re_df = spark.sql("select driverId, sum(points) as total_points, count(points) as total_races from results group by driverId having count(points) > 10 and sum(points) > 100 order by sum(points) desc")
display(re_df)

# COMMAND ----------

 display(df.groupBy("driverId","year").agg(sum("points").alias("total_points"),count("points").alias("total_races")).orderBy(sum("points").desc(), "driverId").filter(col("total_races") > 10).filter(col("total_points") > 100))

# COMMAND ----------

# Group by drive and get total poiints
from pyspark.sql.functions import col, sum, count, countDistinct,rank,dense_rank, lag,lead, desc, row_number, when
from pyspark.sql import Window

df = spark.read.format("delta").load(f"{processed_folder_path}/results_with_drivers_delta/")
df.cache()
df.createOrReplaceTempView("results_drivers")
w_fun = Window.partitionBy("year").orderBy(desc("total_points"), "first_place")
rdf = df.where("year >2018").groupBy("year","name").agg(sum("points").alias("total_points"),count("points").alias("total_races"), count(when(col("position") == 1,True)).alias("first_place")).filter(col("total_races") > 5).filter(col("total_points") > 70)

#display(df.where("year >2018").groupBy("name","year").agg(sum("points").alias("total_points"),count("points").alias("total_races")).orderBy("year",sum("points").desc()).filter(col("total_races") > 10).filter(col("total_points") > 100))
#display(spark.sql("select * from results limit 5"))
#re_df = spark.sql("select name, min(driverId) as id,  year, sum(points) as total_points, count(points) as total_races from results_drivers where year >= 2019 group by name,year having count(points) > 10 and sum(points) > 100 order by sum(points) desc")
rdf = rdf.withColumn("rank", rank().over(w_fun)).withColumn("prev_rank", lag("rank").over(w_fun)).withColumn("next_rank", lead("rank").over(w_fun)).withColumn("row_number", row_number().over(w_fun))
display(rdf)

# COMMAND ----------

display(driver_df.drop("ingestion_dt","source","number").filter(length(col("code")) > 2).join(results_df, "driverId", "anti"))
