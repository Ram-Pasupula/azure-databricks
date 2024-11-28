# Databricks notebook source
# MAGIC %md
# MAGIC #Access azure datalake using Secret utility  (Azure Key vault + DB secrets)
# MAGIC 1. set spark config
# MAGIC 2. list files
# MAGIC 3. read file
# MAGIC
# MAGIC

# COMMAND ----------

help(dbutils.secrets)

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("demo")

# COMMAND ----------

accesskey = dbutils.secrets.get("demo", "azure-db-accesskey")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.azuredbstorageac.dfs.core.windows.net", accesskey)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@azuredbstorageac.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@azuredbstorageac.dfs.core.windows.net/circuits.csv").show()

# COMMAND ----------

#dbutils.fs.rm("abfss://demo@azuredbstorageac.dfs.core.windows.net/circuits.csv")
