# Databricks notebook source
# DBTITLE 1,List and Display File Count in S3 Bucket Path
bucket_path = "s3a://sppevribucket/rawdata3"
files = dbutils.fs.ls(bucket_path)
print(len(files))
display(files)

# COMMAND ----------

# DBTITLE 1,Stream and Process JSON Files from S3 Bucket
from pyspark.sql.functions import input_file_name, regexp_replace

bucket_path = "s3a://sppevribucket/rawdata3"

# Read files using Auto Loader
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.inferSchema", "true")
      .option("multiline", "true")
      .option("cloudFiles.pathGlobFilter", "H*.json")  
      .option("cloudFiles.schemaLocation", f"{bucket_path}/schema4/")
      .option("cloudFiles.validateOptions", "false")
      .load(bucket_path))
# mode permissive
# Add source file information
df = df.withColumn("file_path", input_file_name())
df = df.withColumn("barcode", regexp_replace(input_file_name(), r".*/|\.json", ""))
display_query = display(df)

# Write to a bronze table (uncomment if needed)
streaming_query = df.writeStream.format("delta").option("checkpointLocation", "s3a://sppevribucket/checkpoints/bronze3").table("bronze_table_WO_explode")


# COMMAND ----------

# MAGIC %md
# MAGIC # **Clean Up ( one time cell )**

# COMMAND ----------

# DBTITLE 1,Stop Active Queries and Clean Up Resources

# for query in spark.streams.active:
#     query.stop()
# print("All active streaming queries have been stopped.")

# # Clean up schema, checkpoint, and table
# dbutils.fs.rm(f"{bucket_path}/schema4/", recurse=True)
# dbutils.fs.rm("s3a://sppevribucket/checkpoints/bronze3", recurse=True)
# spark.sql("DROP TABLE IF EXISTS bronze_table_WO_explode")

# # Exit the notebook cell
# dbutils.notebook.exit("Cell execution stopped.")

# COMMAND ----------

# DBTITLE 1,Count Rows in Bronze Table
total_count = spark.table("bronze_table_WO_explode").count()
total_count

# COMMAND ----------

# DBTITLE 1,Generate and Save Parcel Tracking Data to Delta Table
from pyspark.sql import Row
import random

# Generate dummy data
data = []
statuses = ["pending", "departed", "arrived", "in_transit"]
for i in range(1000):
    barcode = f"H00{i}"
    status = random.choice(statuses)
    quantity = random.randint(1, 10)
    location = f"Location_{random.randint(1, 20)}"
    data.append(Row(barcode=barcode, status=status, quantity=quantity, location=location))

# Create DataFrame
df = spark.createDataFrame(data)

# Save DataFrame as Delta table
df.write.format("delta").mode("overwrite").saveAsTable("silver_parcel_tracking")

# Display DataFrame
display(df)

# COMMAND ----------

# DBTITLE 1,Generate & Save Dummy Parcel Information to Delta Table
import random
from datetime import datetime, timedelta

# Generate dummy data
data = []
for i in range(1000):
    tracking_number = f"T00{i}"
    expected_delivery_date = (datetime.now() + timedelta(days=random.randint(1, 10))).strftime('%Y-%m-%d')
    estimated_arrival_time = (datetime.now() + timedelta(hours=random.randint(1, 24))).strftime('%H:%M:%S')
    barcode = f"H00{i}"
    data.append(Row(tracking_number=tracking_number, expected_delivery_date=expected_delivery_date, estimated_arrival_time=estimated_arrival_time, barcode=barcode))

# Create DataFrame
df = spark.createDataFrame(data)

# Save DataFrame as Delta table
df.write.format("delta").mode("overwrite").saveAsTable("silver_parcel_information")

# Display DataFrame
display(df)

# COMMAND ----------

# DBTITLE 1,Join Parcel Data and Save Delivery Table
# Load the tables
df_parcel_info = spark.table("silver_parcel_information")
df_parcel_tracking = spark.table("silver_parcel_tracking")
df_bronze_wo_explode = spark.table("bronze_table_WO_explode")

# Perform the join on the barcode field
df_silver_delivery = df_parcel_info.join(df_parcel_tracking, "barcode").join(df_bronze_wo_explode, "barcode")

# Save the result as a new Delta table
df_silver_delivery.write.format("delta").mode("overwrite").saveAsTable("silver_delivery")

# Display the resulting DataFrame
display(df_silver_delivery)

# COMMAND ----------

# DBTITLE 1,List and Display Columns in Silver Delivery Table
df_silver_delivery_columns = spark.table("silver_delivery").columns
print(df_silver_delivery_columns)

# COMMAND ----------

# DBTITLE 1,Visualize Delivery Metrics
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Load the data into a Pandas DataFrame for visualization
df = spark.table("silver_delivery").toPandas()

# Check if the DataFrame is empty after loading
if df.empty:
    print("The DataFrame is empty after loading from the Spark table.")
else:
    # Convert necessary columns to numeric or datetime
    df['expected_delivery_date'] = pd.to_datetime(df['expected_delivery_date'], errors='coerce')
    df['estimated_arrival_time'] = pd.to_datetime(df['estimated_arrival_time'], errors='coerce')
    df['number_of_parcels_visible'] = pd.to_numeric(df['number_of_parcels_visible'], errors='coerce')
    df['parcel_exposed_to_elements'] = pd.to_numeric(df['parcel_exposed_to_elements'], errors='coerce')

    # Replace NaN values with default values
    df['parcel_exposed_to_elements'] = df['parcel_exposed_to_elements'].fillna(0)
    df['number_of_parcels_visible'] = df['number_of_parcels_visible'].fillna(0)
    df['status'] = df['status'].fillna('Unknown')

    # 1. Delivery Status Distribution
    plt.figure(figsize=(10, 6))
    sns.countplot(data=df, x='status')
    plt.title('Delivery Status Distribution')
    plt.xticks(rotation=45)
    plt.show()

    # 2. Number of Parcels Visible Distribution
    plt.figure(figsize=(10, 6))
    sns.histplot(df['number_of_parcels_visible'], bins=30, kde=True)
    plt.title('Number of Parcels Visible Distribution')
    plt.xlabel('Number of Parcels Visible')
    plt.ylabel('Frequency')
    plt.show()

    # 3. Parcel Exposed to Elements Distribution
    plt.figure(figsize=(10, 6))
    sns.histplot(df['parcel_exposed_to_elements'], bins=30, kde=True)
    plt.title('Parcel Exposed to Elements Distribution')
    plt.xlabel('Parcel Exposed to Elements')
    plt.ylabel('Frequency')
    plt.show()

    # 4. Expected Delivery Date Distribution
    plt.figure(figsize=(10, 6))
    sns.histplot(df['expected_delivery_date'].dropna(), bins=30, kde=True)
    plt.title('Expected Delivery Date Distribution')
    plt.xlabel('Expected Delivery Date')
    plt.ylabel('Frequency')
    plt.xticks(rotation=45)
    plt.show()

    # 5. Estimated Arrival Time Distribution
    plt.figure(figsize=(10, 6))
    sns.histplot(df['estimated_arrival_time'].dropna(), bins=30, kde=True)
    plt.title('Estimated Arrival Time Distribution')
    plt.xlabel('Estimated Arrival Time')
    plt.ylabel('Frequency')
    plt.xticks(rotation=45)
    plt.show()

    # 6. Location Context Distribution
    plt.figure(figsize=(10, 6))
    sns.countplot(data=df, x='location_context')
    plt.title('Location Context Distribution')
    plt.xticks(rotation=45)
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### My experiments

# COMMAND ----------

# DBTITLE 1,List and Display Files in S3 Bucket Path
bucket_path = "s3a://sppevribucket/"
files = dbutils.fs.ls(bucket_path)
print(len(files))
display(files)

# COMMAND ----------

# DBTITLE 1,Read and Display JSON Files from S3 Bucket Path
from pyspark.sql.functions import input_file_name

# Define your S3 path
bucket_path = "s3a://sppevribucket/"

# Read only H1*.json files (ignoring copies and directories)
df = (spark.read
      .format("json")
      .option("pathGlob", "H*.json")  # Matches files starting with H1 and ending with .json
      .option("recursiveFileLookup", "true")  # Searches subdirectories
      .load(bucket_path))

# Add source file information
df = df.withColumn("source_file", input_file_name())

# Show results
print(f"Read {df.count()} records from matching files")
display(df)

# COMMAND ----------

# DBTITLE 1,Read and Display Specific JSON File from S3 Path
# Define the path to the specific JSON file
file_path = f"s3a://sppevribucket/rawdata3/H00999.json"

# Read the JSON file into a DataFrame
df = spark.read.option("multiline", "true").json(file_path)

# Display the content of the DataFrame
display(df)
