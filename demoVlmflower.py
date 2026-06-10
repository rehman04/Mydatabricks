# Databricks notebook source
import json
import mlflow

import pyspark.sql.functions as F

from mlflow.deployments import get_deploy_client

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/flower_photos/'))

# COMMAND ----------

display(dbutils.fs.mounts())  # No mount point for public datasets

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/flower_photos/dandelion/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG flowervlmdemo
# MAGIC MANAGED LOCATION 's3://databricks-workspace-stack-11de3-bucket'
# MAGIC COMMENT 'Flower Vlm Demo';
# MAGIC

# COMMAND ----------

'CREATE CATALOG myCatalog MANAGED LOCATION '<location-path>'

# COMMAND ----------

spark.sql("CREATE CATALOG flowervlmdemo")

# COMMAND ----------

spark.sql("DESCRIBE EXTENDED stuart")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists stuart.evri;

# COMMAND ----------

raw_images_sdf = (
  spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "binaryFile")
  .option("cloudFiles.maxFilesPerTrigger", "2") # this is the first of our throttles
  .option("recursiveFileLookup", "true")
  .option("pathGlobFilter", "*.jpg")
  .load("dbfs:/databricks-datasets/flower_photos/")
  .withColumn("b64", F.base64("content")) # create a base64 encoded duplicate of the `content` column
  )

display(raw_images_sdf)

# COMMAND ----------

# store a small number of the image contents so we can test the model

image_table_ref = "stuart.evri.image_raw"

(
  raw_images_sdf
  .limit(20)
  .writeStream
  .trigger(processingTime="1 minutes")
  .option("checkpointLocation", f"dbfs:/tmp/checkpoints/{image_table_ref}")
  .toTable(image_table_ref)
  )

# COMMAND ----------

# Make a request to the custom model serving endpoint hosting a VLM
model_name = "databricks-claude-3-7-sonnet"
prompt = "Please take a look at the picture and describe the type of flower present and its surroundings."
image_base64 = spark.table(image_table_ref).first()["b64"]

# COMMAND ----------

client = get_deploy_client("databricks")
response = client.predict(
    endpoint=model_name,
    inputs={
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/png;base64,{image_base64}"},
                },
                ]
            }
        ],
        "max_tokens":200   # limit the output tokens
    }
)
print(response)

# COMMAND ----------

# wrap this call as a spark UDF
@F.udf("string")
def get_response(prompt, image_base64):
  response = client.predict(
    endpoint=model_name,
    inputs={
        "messages": [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url","image_url": {"url": f"data:image/png;base64,{image_base64}"},},
            ],
        }
    ],
    },
)
  return json.dumps(response)

# COMMAND ----------

# test it out
raw_images_sdf.limit(1).withColumn("response", get_response(F.lit(prompt), F.col("b64"))).display()

# COMMAND ----------

response_table_ref = f"stuart.evri.image_response"

(
  raw_images_sdf
  .withColumn("response", get_response(F.lit(prompt), F.col("b64"))) # call the UDF
  .writeStream
  .trigger(processingTime="1 minutes") # the other control over throughput
  .option("checkpointLocation", f"dbfs:/tmp/checkpoints/{response_table_ref}")
  .toTable(response_table_ref)
  )

# COMMAND ----------

display(spark.table(response_table_ref))

# COMMAND ----------

