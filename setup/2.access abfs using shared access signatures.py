# Databricks notebook source
# MAGIC %md
# MAGIC #Access azure datalake using accesskeys (provides access to entire storage account)
# MAGIC 1. set spark config
# MAGIC 2. list files
# MAGIC 3. read file
# MAGIC
# MAGIC .Unlike Access keys, Shared Access Signatures can be used to control access at a more granular level.
# MAGIC
# MAGIC .We can restrict access to specific resource types or services.
# MAGIC
# MAGIC .For example, we can allow access to only Blob containers, thus restricting access to files, queues,
# MAGIC
# MAGIC tables, etc.
# MAGIC
# MAGIC .Also, we can allow only specific permissions.
# MAGIC
# MAGIC .For example, we can allow Read only access to a container, thus restricting the user from writing
# MAGIC
# MAGIC or deleting the Blobs.
# MAGIC
# MAGIC .It even allows us to restrict the time period during which the user has access to the data.
# MAGIC
# MAGIC .And also, we can allow access to specific IP addresses, thus avoiding public access.
# MAGIC
# MAGIC .With this level of fine grained access control offered, SAS tokens are the recommended access pattern
# MAGIC
# MAGIC .for external clients, who cannot be trusted with full access to your storage account.
# MAGIC
# MAGIC There are different types of tokens too.
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.secrets.list(scope="demo")

# COMMAND ----------

sas_token = dbutils.secrets.get(scope="demo", key="storage-demo-sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.azuredbstorageac.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.azuredbstorageac.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.azuredbstorageac.dfs.core.windows.net", sas_token)

# COMMAND ----------


dbutils.fs.ls("abfss://demo@azuredbstorageac.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@azuredbstorageac.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

#dbutils.fs.rm("abfss://demo@azuredbstorageac.dfs.core.windows.net/circuits.csv")
