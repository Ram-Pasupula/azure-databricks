# Databricks notebook source
# MAGIC %run "../includes/config/configuration"

# COMMAND ----------

dbutils.widgets.text("folder_nm", "")

# COMMAND ----------

raw_folder_path

# COMMAND ----------

folder_nm = dbutils.widgets.get("folder_nm") 
folder_nm


# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

status = dbutils.notebook.run("./1.access adfs using accesskeys", 100, {"folder_nm": folder_nm})
status
