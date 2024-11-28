# Databricks notebook source
# MAGIC %run "../includes/config/configuration/"

# COMMAND ----------

df = spark.read.format("delta").load(f"{processed_folder_path}/circuits_delta")

# COMMAND ----------

df.show(5, truncate=False)

# COMMAND ----------

sdf  = spark.readStream.format("delta").load(f"{processed_folder_path}/circuits_delta")


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit
sdf_ts = sdf.withColumn("ts",current_timestamp()).withColumn("source", lit("results_ext_delta"))
                                                            


# COMMAND ----------

display(sdf_ts)

# COMMAND ----------

query = (sdf_ts.writeStream.format("delta").outputMode("append").option("checkpointLocation", f"{processed_folder_path}/checkpoint").option("queryName", "delta_circuits_stream").toTable("delta_circuits_stream"))
#.start(f"{processed_folder_path}/delta_circuits_stream")


# COMMAND ----------

display(query.name)

# COMMAND ----------

query.status
