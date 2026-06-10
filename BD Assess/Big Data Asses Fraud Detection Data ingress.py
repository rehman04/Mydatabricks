-- Databricks notebook source
-- MAGIC %md
-- MAGIC Reading transactions from the Volume within the catalog From Cloud Storage

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC     spark.read.json('/Volumes/asscom1fraudanalysis/bdcom1asses/fraudanalysis/transactions/')
-- MAGIC     .limit(10)  # ← Most efficient
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Reading Customers From Cloud Storage

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.csv('/Volumes/asscom1fraudanalysis/bdcom1asses/fraudanalysis/customers', header=True, multiLine=True).limit(10))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Reading country_code From Cloud Storage

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC     spark.read.csv(
-- MAGIC         '/Volumes/asscom1fraudanalysis/bdcom1asses/fraudanalysis/country_code',
-- MAGIC         header=True
-- MAGIC     ).limit(10)  # ← Critical: Stops after 10 records
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Reading Fraud Report From Cloud Storage

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC     spark.read.csv(
-- MAGIC         '/Volumes/asscom1fraudanalysis/bdcom1asses/fraudanalysis/fraud_report',
-- MAGIC         header=True,
-- MAGIC         multiLine=True  # Required if fields contain newlines
-- MAGIC     ).limit(10)  # ← Critical: Reads only 10 records
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating Raw transaction table from Cloud files

-- COMMAND ----------


CREATE OR REFRESH STREAMING LIVE TABLE bronze_transactions 
  COMMENT "Historical banking transaction to be trained on fraud detection"
AS 
  SELECT * FROM cloud_files(
    "/Volumes/asscom1fraudanalysis/bdcom1asses/fraudanalysis/transactions/", 
    "json", 
    map(
      "cloudFiles.maxFilesPerTrigger", "1", 
      "cloudFiles.inferColumnTypes", "true"
    )
  )


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating Raw Customers table from Cloud files

-- COMMAND ----------


CREATE STREAMING LIVE TABLE banking_customers (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "Customer data coming from csv files ingested in incremental with Auto Loader to support schema inference and evolution"
AS 
  SELECT * FROM cloud_files(
    "/Volumes/asscom1fraudanalysis/bdcom1asses/fraudanalysis/customers/", 
    "csv", 
    map(
      "cloudFiles.inferColumnTypes", "true", 
      "multiLine", "true"
    )
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating Raw Countries table from Cloud files

-- COMMAND ----------



CREATE STREAMING LIVE TABLE country_coordinates
AS 
  SELECT * FROM cloud_files("/Volumes/asscom1fraudanalysis/bdcom1asses/fraudanalysis/country_code/", "csv")


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating Raw Fraud Reports table from Cloud files for ML

-- COMMAND ----------


CREATE STREAMING LIVE TABLE fraud_reports
AS 
  SELECT * FROM cloud_files("/Volumes/asscom1fraudanalysis/bdcom1asses/fraudanalysis/fraud_report/", "csv")



-- COMMAND ----------

-- MAGIC %md
-- MAGIC Enforcing Quality on Transactions and Fraud Reports to make a Silver Table
-- MAGIC
-- MAGIC This SQL defines a streaming Delta Lake table named silver_transactions that streams data in real-time from source table bronze_transactions. It maintains data quality by ensuring that fields id and customer_id are never null by enforcing constraint checks. The data transformation logic omits double hyphens (--), removes them from countryOrig and countryDest columns, computes differences in balance for origin and destination accounts, and performs an inner join with a fraud reports table to enhance the data. The query avoids duplicate columns such as _rescued_data and adopts a streaming data approach of processing new records iteratively while executing efficient and current data processing. This arrangement is typical in medallion layouts wherein raw information (bronze) is refined to a purer, validated state (silver).

-- COMMAND ----------


CREATE STREAMING LIVE TABLE silver_transactions (
  CONSTRAINT correct_data EXPECT (id IS NOT NULL),
  CONSTRAINT correct_customer_id EXPECT (customer_id IS NOT NULL)
)
AS 
  SELECT * EXCEPT(countryOrig, countryDest, t._rescued_data, f._rescued_data), 
          regexp_replace(countryOrig, "\-\-", "") as countryOrig, 
          regexp_replace(countryDest, "\-\-", "") as countryDest, 
          newBalanceOrig - oldBalanceOrig as diffOrig, 
          newBalanceDest - oldBalanceDest as diffDest
FROM STREAM(live.bronze_transactions) t
  LEFT JOIN live.fraud_reports f using(id)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Making a table for ML Features Gold Table Features that are Required for the Fraud Detection
-- MAGIC
-- MAGIC This SQL query produces a production-grade Delta table named gold_transactions that appends customer and geog data to transactional data with data integrity maintained. It has a constraint to keep amounts on transactions greater than 10 (amount_decent). It is merging data from a number of tables: cleaned silver_transactions table with country coordinate data (destination country and origin country) to append geographic data like country, latitude, and longitude, and customer banking to append customer. Data cleaning is done by dropping unnecessary columns, renaming fraud indicator column type to the correct boolean, and renaming fields for ease. This is the last ("gold") layer in a medallion composition, wherein raw data have been converted into an analysis-capable dataset with full geographic and customer context suitable for business reporting and fraud detection analytics. Inner joins produce records with valid customer IDs and country codes only.

-- COMMAND ----------


CREATE LIVE TABLE gold_transactions (
  CONSTRAINT amount_decent EXPECT (amount > 10)
)
AS 
  SELECT t.* EXCEPT(countryOrig, countryDest, is_fraud), c.* EXCEPT(id, _rescued_data),
          boolean(coalesce(is_fraud, 0)) as is_fraud,
          o.alpha3_code as countryOrig, o.country as countryOrig_name, o.long_avg as countryLongOrig_long, o.lat_avg as countryLatOrig_lat,
          d.alpha3_code as countryDest, d.country as countryDest_name, d.long_avg as countryLongDest_long, d.lat_avg as countryLatDest_lat
FROM live.silver_transactions t
  INNER JOIN live.country_coordinates o ON t.countryOrig=o.alpha3_code 
  INNER JOIN live.country_coordinates d ON t.countryDest=d.alpha3_code 
  INNER JOIN live.banking_customers c ON c.id=t.customer_id 