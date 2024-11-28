# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE customers
# MAGIC AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/customers/", "csv");
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE sales_orders_raw
# MAGIC AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/sales_orders/", "json");

# COMMAND ----------

# MAGIC %md
# MAGIC Silver layer- join

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE STREAMING LIVE TABLE sales_orders AS (
# MAGIC   SELECT c.customer_id, c.customer_name, c.units_purchased, s.order_number, s.ordered_products  FROM STREAM(LIVE.customers) c JOIN LIVE.sales_orders_raw s ON c.customer_id = s.customer_id);

# COMMAND ----------

display(_sqldf)

# COMMAND ----------

# MAGIC %md
# MAGIC Load raw data into tables

# COMMAND ----------

# import dlt
# @dlt.table
# def customers():
#   return (
#     spark.readStream.format("cloudFiles")
#       .option("cloudFiles.format", "csv")
#       .load("/databricks-datasets/retail-org/customers/")
#   )

# @dlt.table
# def sales_orders_raw():
#   return (
#     spark.readStream.format("cloudFiles")
#       .option("cloudFiles.format", "json")
#       .load("/databricks-datasets/retail-org/sales_orders/")
#   )

