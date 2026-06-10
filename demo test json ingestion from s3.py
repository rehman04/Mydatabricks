# Databricks notebook source
# Path to the S3 bucket
s3_path = "s3a://sppevribucket/"

# Recursive function to list all files in folders and subfolders
def list_files_recursive(path):
    files_and_folders = dbutils.fs.ls(path)
    for item in files_and_folders:
        if item.isDir():
            print(f"Directory: {item.path}")
            list_files_recursive(item.path)  # Recursive call for subdirectories
        else:
            print(f"File: {item.path}")

# List all files and folders recursively
list_files_recursive(s3_path)

# COMMAND ----------

from pyspark.sql.functions import *

# Path to the S3 bucket directory
s3_path = "s3a://sppevribucket/output/"

# Configure Auto Loader to read JSON files with schema evolution
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("cloudFiles.includeExistingFiles", "true") \
    .option("cloudFiles.schemaLocation", "s3a://sppevribucket/schema-json-demo-location/") \
    .load(s3_path)

# Write the data to a Bronze Delta table with schema evolution and checkpointing
df.writeStream.format("delta") \
    .option("checkpointLocation", "s3a://sppevribucket/checkpoints/bronze_json_demo/") \
    .outputMode("append") \
    .table("eu_west2_space.default.bronze_json_demo")


# COMMAND ----------

from pyspark.sql.functions import *

# Path to the S3 bucket directory
s3_path = "s3a://sppevribucket/output/"

# Configure Auto Loader to read JSON files
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.includeExistingFiles", "true") \
    .load(s3_path)

# Display the schema of each file
df.printSchema()


# COMMAND ----------

# Set up Auto Loader for continuous ingestion
(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .option("cloudFiles.schemaLocation", "s3a://sppevribucket/schema-json-demo-location/")  # Stores schema
 .option("cloudFiles.inferColumnTypes", "true")
 .load("s3a://sppevribucket/output/").schema("<your_schema_here>")
 .writeStream
 .option("checkpointLocation", f"s3a://sppevribucket/schema-json-demo-location/_checkpoint")
 .trigger(availableNow=True)  # Change to continuous for real-time
 .toTable("eu_west2_space.default.bronze_json_demo"))

# COMMAND ----------

from pyspark.sql.functions import *

# Path to the S3 bucket directory
s3_path = "s3a://sppevribucket/output/"

# Configure Auto Loader to read JSON files
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.includeExistingFiles", "true") \
    .load("s3a://sppevribucket/output/H1234567890.json")

# Display the schema of each file
df.printSchema()


# COMMAND ----------

from pyspark.sql import SparkSession

# Path to the specific JSON file
file_path = "s3a://sppevribucket/output/H1234567892.json"

# Read the JSON file
df = spark.read.format("json").load(file_path)

# Display the schema of the file
df.printSchema()


# COMMAND ----------

df

# COMMAND ----------

# Path to the specific JSON file
file_path = "s3a://sppevribucket/output/H1234567890.json"

# Read the JSON file with corrupt record handling
df = spark.read.option("badRecordsPath", "s3a://sppevribucket/bad-records/").json(file_path)

# Display the schema of the file
df.printSchema()

# Show the data to verify
df.show(truncate=False)
