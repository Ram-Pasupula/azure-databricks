-- Databricks notebook source
create database if not exists raw;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS raw.race (
circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt  INT,
url STRING
) using csv options (path "abfss://raw@azuredbstorageac.dfs.core.windows.net/circuits.csv", header "true");

-- COMMAND ----------

--SELECT circuitId,  name FROM raw.race limit 5;

select circuitId, split(name, ' ')[0] as names FROM raw.race limit 5;

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

DROP TABLE IF EXISTS raw.driver;
CREATE TABLE IF NOT EXISTS raw.driver( 
  driverId INT,
  driverRef STRING, 
  number  int,
  code  string,
  --name string,
  name struct<forename VARCHAR(50),surname string>,
  dob DATE,
  nationality VARCHAR(50),
  url string
) using json options (path "abfss://raw@azuredbstorageac.dfs.core.windows.net/drivers.json")

-- COMMAND ----------

SELECT driverId ,
 
  name.forename as name  FROM raw.driver limit 5;

-- COMMAND ----------



-- COMMAND ----------

DROP TABLE IF EXISTS raw.pits;
CREATE TABLE IF NOT EXISTS raw.pits( 
  raceId INT,
   driverId int,
   stop int,
    lap int,
     time string,
     duration DOUBLE,
     milliseconds long
) using json options (path "abfss://raw@azuredbstorageac.dfs.core.windows.net/pit_stops.json" , multiLine=true)

-- COMMAND ----------

SELECT * FROM raw.pits limit 5

-- COMMAND ----------

SHow catalogs

-- COMMAND ----------

create database if not exists az_databrics_ws.processed MANAGED LOCATION  "abfss://processed@azuredbstorageac.dfs.core.windows.net/processed";

-- COMMAND ----------

SHOW SCHEMAS FROM az_databrics_ws

-- COMMAND ----------

select 1;
