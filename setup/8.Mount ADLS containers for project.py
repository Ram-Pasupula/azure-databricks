# Databricks notebook source
# MAGIC %md
# MAGIC #Access azure datalake using accesskeys (provides access to entire storage account)
# MAGIC 1. set spark config
# MAGIC 2. list files
# MAGIC 3. read file
# MAGIC .First, we need to register the Service Principal(App registration).(https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationMenuBlade/~/Overview/appId/f983881e-70d1-4abd-b4c0-3e362f76c60a/isMSAApp~/false)
# MAGIC Display name
# MAGIC :
# MAGIC myserviceacc-app
# MAGIC Application (client) ID
# MAGIC :
# MAGIC f983881e-70d1-4abd-b4c0-3e362f76c60a
# MAGIC Object ID
# MAGIC :
# MAGIC 23469ccf-428b-4700-acf1-a2bc2352fa5c
# MAGIC Directory (tenant) ID
# MAGIC :
# MAGIC bcbc4283-cbbf-4c7a-9497-7df9b4e7fbe1
# MAGIC
# MAGIC
# MAGIC -> Generate. a secret password for app.
# MAGIC
# MAGIC We then need to configure Databricks to access the storage account via the Service Principal.
# MAGIC
# MAGIC We then need to assign the required role on the Data Lake for the Service Principal so that the Service Principal can access the data in the Data Lake.
# MAGIC
# MAGIC The Role, Storage Blob Data Contributor, gives full access to the storage account,
# MAGIC

# COMMAND ----------

# MAGIC %run  "../includes/utilities"
# MAGIC  
# MAGIC

# COMMAND ----------

mount_adls("azuredbstorageac", "demo")
mount_adls("azuredbstorageac","raw")
mount_adls("azuredbstorageac","processed")

# COMMAND ----------


dbutils.fs.ls("/mnt/azuredbstorageac/")

# COMMAND ----------

display(spark.read.csv("/mnt/azuredbstorageac/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/azuredbstorageac")

# COMMAND ----------

#dbutils.fs.rm("abfss://demo@azuredbstorageac.dfs.core.windows.net/circuits.csv")
