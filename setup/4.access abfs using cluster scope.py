# Databricks notebook source
# MAGIC %md
# MAGIC #Access azure datalake using accesskeys (provides access to entire storage account)
# MAGIC 1. set spark config in compute cluster adv options
# MAGIC 2. list files
# MAGIC 3. read file
# MAGIC .First, we need to register the Service Principal.
# MAGIC Display name
# MAGIC
# MAGIC

# COMMAND ----------

# using Secrets - {{secrets/secrets_scope/secretName}}
# in spark conf of cluster
# fs.azure.account.key.azuredbstorageac.dfs.core.windows.net  {{secrets/secrets_scope/secretName}}

# COMMAND ----------


dbutils.fs.ls("abfss://demo@azuredbstorageac.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@azuredbstorageac.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

#dbutils.fs.rm("abfss://demo@azuredbstorageac.dfs.core.windows.net/circuits.csv")
