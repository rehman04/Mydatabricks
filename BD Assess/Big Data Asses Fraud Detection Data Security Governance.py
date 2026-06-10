# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG asscom1fraudanalysis;

# COMMAND ----------

# MAGIC %md
# MAGIC Creating User Groups For Access Control List

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE GROUP analysts WITH USER  `shifajamali55@gmail.com`; 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE GROUP dataengineers WITH USER  `shifajamali55@gmail.com`; 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW GROUPS;

# COMMAND ----------

# MAGIC %md
# MAGIC Granting Access to different user groups

# COMMAND ----------

# MAGIC %sql
# MAGIC -- - Grant SELECT permission to the 'analysts' group
# MAGIC GRANT SELECT ON TABLE asscom1fraudanalysis.bdcom1asses.gold_transactions TO analysts;
# MAGIC -- Grant SELECT and MODIFY permissions to the 'dataengineers' group
# MAGIC GRANT SELECT, MODIFY ON SCHEMA asscom1fraudanalysis.bdcom1asses TO `dataengineers`;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC GRANT ALL PRIVILEGES ON TABLE asscom1fraudanalysis.bdcom1asses.gold_transactions 
# MAGIC TO `shifajamali55@gmail.com`
# MAGIC