# Databricks notebook source
# We've created the delta tables with the sample data set in the previous notebook task.
# Let's take a look at how our sample data looks like, then we'll apply filters and create a report.

# We can first check what was imported in the first task by getting the task values emitted from it.
dbutils.jobs.taskValues.get(taskKey = "Ingestion", key = "count", default = 100, debugValue = "")

# Let's load the table and get a description
loans = spark.read.format("delta").table("loans")
display(loans.describe().toPandas())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's take a look at loan volume and count by loan status with this SQL query
# MAGIC select
# MAGIC   l.loan_status,
# MAGIC   zip_code,
# MAGIC   count(l.loan_amnt) as count,
# MAGIC   sum(l.loan_amnt) as volume
# MAGIC from loans l
# MAGIC group by loan_status, zip_code
# MAGIC order by 4 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's take a look at loan volume and count by loan status with this SQL query
# MAGIC select
# MAGIC   l.loan_status,
# MAGIC   l.loan_amnt,
# MAGIC   l.loan_amnt
# MAGIC from loans l