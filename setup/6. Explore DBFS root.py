# Databricks notebook source
display(dbutils.fs.ls('/FileStore/tables/'))

# COMMAND ----------

display(spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/FileStore/tables/"))
