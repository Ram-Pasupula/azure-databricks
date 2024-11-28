# Databricks notebook source
# MAGIC %md
# MAGIC Read and write to mysql
# MAGIC

# COMMAND ----------

# MAGIC %sh nc -vz 192.168.0.22 3306

# COMMAND ----------

driver = "com.mysql.cj.jdbc.Driver"

database_host = "localhost"
database_port = "3306" # update if you use a non-default port
database_name = "customers"
table = "races"
user = "root"
password = "pasupula"

url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}?useSSL=false&allowPublicKeyRetrieval=true"

remote_table = (spark.read
  .format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("dbtable", table)
  .option("user", user)
  .option("password", password)
  .load()
)


# COMMAND ----------

remote_table = (spark.read
  .format("mysql")
  .option("dbtable", "table_name")
  .option("host", "database_hostname")
  .option("port", "3306") # Optional - will use default port 3306 if not specified.
  .option("database", "database_name")
  .option("user", "username")
  .option("password", "password")
  .load()
)

