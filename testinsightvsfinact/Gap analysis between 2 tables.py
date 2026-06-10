# Databricks notebook source
!pip install openpyxl

# COMMAND ----------

import pandas as pd


# Path to the Excel file in a Databricks volume (Unity Catalog or DBFS)
file_path = "/Volumes/insights_dev_003/dwh/rawdataforboth/FinActVsInsights-Tables_Columns.xlsx"

# Read the Excel file
df = pd.read_excel(file_path, sheet_name='FinAct', engine='openpyxl')  # Use engine='openpyxl' for .xlsx
df.columns = [col.strip().replace(' ', '_').replace('\n', '_') for col in df.columns]
display(df)


# COMMAND ----------

spark_df = spark.createDataFrame(df)

# Step 3: Write the Spark DataFrame to a Delta table in Unity Catalog
spark_df.write.format('delta').mode('overwrite').saveAsTable('insights_dev_003.finactdm.tbl_dim_policy_section')


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE insights_dev_003.finactdm.tbl_dim_policy_section;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE insights_dev_003.dwh.dim_policy_section;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC real work

# COMMAND ----------

# MAGIC %sql
# MAGIC -- schema of Finact ( source of truth )
# MAGIC WITH finact_schema AS (
# MAGIC     SELECT 
# MAGIC         table_name,
# MAGIC         column_name, 
# MAGIC         data_type
# MAGIC     FROM insights_dev_003.information_schema.columns
# MAGIC     WHERE table_schema = 'finactdm' 
# MAGIC       AND table_name = 'tbl_dim_policy_section'
# MAGIC ),
# MAGIC
# MAGIC -- schema of DWH ( insights )
# MAGIC dwh_schema AS (
# MAGIC     SELECT 
# MAGIC         table_name,
# MAGIC         column_name, 
# MAGIC         data_type
# MAGIC     FROM insights_dev_003.information_schema.columns
# MAGIC     WHERE table_schema = 'dwh' 
# MAGIC       AND table_name = 'dim_policy_section'
# MAGIC ),
# MAGIC
# MAGIC -- Comparing missing or mismatched in ( insights )
# MAGIC finact_vs_dwh AS (
# MAGIC     SELECT 
# MAGIC         f.column_name,
# MAGIC         f.data_type AS finact_type,
# MAGIC         d.data_type AS dwh_type,
# MAGIC         CASE 
# MAGIC             WHEN d.column_name IS NULL THEN 'Missing in DWH'
# MAGIC             WHEN f.data_type <> d.data_type THEN 'Data type mismatch'
# MAGIC             ELSE 'Unknown'
# MAGIC         END AS mismatch_reason
# MAGIC     FROM finact_schema f
# MAGIC     LEFT JOIN dwh_schema d
# MAGIC         ON f.column_name = d.column_name
# MAGIC        AND d.table_name = 'dim_policy_section'
# MAGIC     WHERE d.column_name IS NULL OR f.data_type <> d.data_type
# MAGIC ),
# MAGIC
# MAGIC -- Compare extra in insights
# MAGIC dwh_vs_finact AS (
# MAGIC     SELECT 
# MAGIC         d.column_name,
# MAGIC         NULL AS finact_type,
# MAGIC         d.data_type AS dwh_type,
# MAGIC         'Extra in DWH' AS mismatch_reason
# MAGIC     FROM dwh_schema d
# MAGIC     LEFT JOIN finact_schema f
# MAGIC         ON d.column_name = f.column_name
# MAGIC        AND f.table_name = 'tbl_dim_policy_section'
# MAGIC     WHERE f.column_name IS NULL
# MAGIC )
# MAGIC
# MAGIC -- Final
# MAGIC SELECT * FROM finact_vs_dwh
# MAGIC UNION ALL
# MAGIC SELECT * FROM dwh_vs_finact;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Row wise comparison

# COMMAND ----------

# MAGIC %md
# MAGIC explicit columns

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COALESCE(i.Policy_Section_Reference, f.Policy_Section_Reference) AS SectionRef,
# MAGIC     i.Policy_Header_Reference AS iHead,
# MAGIC     f.Policy_Header_Reference AS fHead,
# MAGIC     i.Inception_Date AS iDate,
# MAGIC     f.Inception_Date AS fDate,
# MAGIC     CASE 
# MAGIC         WHEN i.Policy_Section_Reference IS NULL THEN 'Finact only'
# MAGIC         WHEN f.Policy_Section_Reference IS NULL THEN 'Insights only'
# MAGIC         WHEN i.Policy_Header_Reference <> f.Policy_Header_Reference 
# MAGIC              OR i.Inception_Date <> f.Inception_Date THEN 'Mismatch'
# MAGIC         ELSE 'Same'
# MAGIC     END AS Status
# MAGIC FROM insights_dev_003.finactdm.tbl_dim_policy_section i
# MAGIC FULL JOIN insights_dev_003.dwh.dim_policy_section f
# MAGIC     ON i.Policy_Section_Reference = f.Policy_Section_Reference;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC sql plus python implementation

# COMMAND ----------

# Define table names
table1 = "insights_dev_003.finactdm.tbl_dim_policy_section"
table2 = "insights_dev_003.dwh.dim_policy_section"
join_key = "Policy_Section_Reference"

# Step 1: Get common columns from both tables (excluding join key)
columns1 = [field.name for field in spark.table(table1).schema]
columns2 = [field.name for field in spark.table(table2).schema]
common_columns = list(set(columns1) & set(columns2))
if join_key in common_columns:
    common_columns.remove(join_key)

# Step 2: Create column select and comparison strings
column_selects = ",\n        ".join([
    f"i.{col} AS i_{col}, f.{col} AS f_{col}" for col in common_columns
])
comparison_conditions = " OR\n            ".join([
    f"i_{col} IS DISTINCT FROM f_{col}" for col in common_columns
])

# Step 3: Construct SQL query
final_sql = f"""
WITH CombinedData AS (
    SELECT 
        COALESCE(i.{join_key}, f.{join_key}) AS {join_key},
        i.{join_key} AS i_{join_key},
        f.{join_key} AS f_{join_key},
        {column_selects}
    FROM {table1} i
    FULL OUTER JOIN {table2} f
        ON i.{join_key} = f.{join_key}
),
Comparison AS (
    SELECT 
        {join_key},
        CASE 
            WHEN i_{join_key} IS NULL THEN 'Missing in Finact'
            WHEN f_{join_key} IS NULL THEN 'Missing in Insights'
            WHEN {comparison_conditions} THEN 'Mismatch in Column Values'
            ELSE 'Exact Match'
        END AS Status
    FROM CombinedData
)
SELECT * FROM Comparison
"""

# Step 4: Run and display result
display(spark.sql(final_sql))


# COMMAND ----------

# %sql
# WITH table1_cols AS (
#     SELECT column_name
#     FROM insights_dev_003.information_schema.columns
#     WHERE table_schema = 'finactdm'
#       AND table_name = 'tbl_dim_policy_section'
# ),
# table2_cols AS (
#     SELECT column_name
#     FROM insights_dev_003.information_schema.columns
#     WHERE table_schema = 'dwh'
#       AND table_name = 'dim_policy_section'
# ),
# common_cols AS (
#     SELECT t1.column_name
#     FROM table1_cols t1
#     INNER JOIN table2_cols t2
#         ON t1.column_name = t2.column_name
#     WHERE t1.column_name != 'Policy_Section_Reference'
# ),
# expressions AS (
#     SELECT
#         concat_ws(',\n    ',
#             collect_list('i.' || column_name || ' AS i_' || column_name || ', f.' || column_name || ' AS f_' || column_name)
#         ) AS column_selects,
#         concat_ws(' OR\n    ',
#             collect_list('i_' || column_name || ' IS DISTINCT FROM f_' || column_name)
#         ) AS comparison_conditions
#     FROM common_cols
# )
# SELECT '
# WITH CombinedData AS (
#     SELECT 
#         COALESCE(i.Policy_Section_Reference, f.Policy_Section_Reference) AS Policy_Section_Reference,
#         i.Policy_Section_Reference AS i_Policy_Section_Reference,
#         f.Policy_Section_Reference AS f_Policy_Section_Reference,
#         ' || column_selects || '
#     FROM insights_dev_003.finactdm.tbl_dim_policy_section i
#     FULL OUTER JOIN insights_dev_003.dwh.dim_policy_section f
#         ON i.Policy_Section_Reference = f.Policy_Section_Reference
# ),
# Comparison AS (
#     SELECT 
#         Policy_Section_Reference,
#         CASE 
#             WHEN i_Policy_Section_Reference IS NULL THEN ''Missing in Finact''
#             WHEN f_Policy_Section_Reference IS NULL THEN ''Missing in Insights''
#             WHEN ' || comparison_conditions || ' THEN ''Mismatch in Column Values''
#             ELSE ''Exact Match''
#         END AS Status
#     FROM CombinedData
# )
# SELECT * FROM Comparison;' AS final_sql
# FROM expressions;


# COMMAND ----------

# %sql

# WITH CombinedData AS (
#     SELECT 
#         COALESCE(i.Policy_Section_Reference, f.Policy_Section_Reference) AS Policy_Section_Reference,
#         i.Policy_Section_Reference AS i_Policy_Section_Reference,
#         f.Policy_Section_Reference AS f_Policy_Section_Reference,
#         i.Aggregated_Data_Input_Policy_Indicator AS i_Aggregated_Data_Input_Policy_Indicator, f.Aggregated_Data_Input_Policy_Indicator AS f_Aggregated_Data_Input_Policy_Indicator,
#     i.Attachment_Priority AS i_Attachment_Priority, f.Attachment_Priority AS f_Attachment_Priority,
#     i.Bulk_Policy_Indicator AS i_Bulk_Policy_Indicator, f.Bulk_Policy_Indicator AS f_Bulk_Policy_Indicator,
#     i.Credit_Period AS i_Credit_Period, f.Credit_Period AS f_Credit_Period,
#     i.Expiry_Date AS i_Expiry_Date, f.Expiry_Date AS f_Expiry_Date,
#     i.Inception_Date AS i_Inception_Date, f.Inception_Date AS f_Inception_Date,
#     i.Long_Term_Agreement_Expiry_Date AS i_Long_Term_Agreement_Expiry_Date, f.Long_Term_Agreement_Expiry_Date AS f_Long_Term_Agreement_Expiry_Date,
#     i.Notice_Period AS i_Notice_Period, f.Notice_Period AS f_Notice_Period,
#     i.Novated_Policy_Indicator AS i_Novated_Policy_Indicator, f.Novated_Policy_Indicator AS f_Novated_Policy_Indicator,
#     i.Payment_Assignment_Policy_Indicator AS i_Payment_Assignment_Policy_Indicator, f.Payment_Assignment_Policy_Indicator AS f_Payment_Assignment_Policy_Indicator,
#     i.Policy_Header_Reference AS i_Policy_Header_Reference, f.Policy_Header_Reference AS f_Policy_Header_Reference,
#     i.Policy_Reference AS i_Policy_Reference, f.Policy_Reference AS f_Policy_Reference,
#     i.Renewable_Indicator AS i_Renewable_Indicator, f.Renewable_Indicator AS f_Renewable_Indicator,
#     i.Settlement_Frequency AS i_Settlement_Frequency, f.Settlement_Frequency AS f_Settlement_Frequency,
#     i.Sub_Class_Code AS i_Sub_Class_Code, f.Sub_Class_Code AS f_Sub_Class_Code,
#     i.Unrecognised_External_Policy_Indicator AS i_Unrecognised_External_Policy_Indicator, f.Unrecognised_External_Policy_Indicator AS f_Unrecognised_External_Policy_Indicator,
#     i.Year_Of_Account AS i_Year_Of_Account, f.Year_Of_Account AS f_Year_Of_Account
#     FROM insights_dev_003.finactdm.tbl_dim_policy_section i
#     FULL OUTER JOIN insights_dev_003.dwh.dim_policy_section f
#         ON i.Policy_Section_Reference = f.Policy_Section_Reference
# ),
# Comparison AS (
#     SELECT 
#         Policy_Section_Reference,
#         CASE 
#             WHEN i_Policy_Section_Reference IS NULL THEN Missing in Finact
#             WHEN f_Policy_Section_Reference IS NULL THEN Missing in Insights
#             WHEN i_Aggregated_Data_Input_Policy_Indicator IS DISTINCT FROM f_Aggregated_Data_Input_Policy_Indicator OR
#     i_Attachment_Priority IS DISTINCT FROM f_Attachment_Priority OR
#     i_Bulk_Policy_Indicator IS DISTINCT FROM f_Bulk_Policy_Indicator OR
#     i_Credit_Period IS DISTINCT FROM f_Credit_Period OR
#     i_Expiry_Date IS DISTINCT FROM f_Expiry_Date OR
#     i_Inception_Date IS DISTINCT FROM f_Inception_Date OR
#     i_Long_Term_Agreement_Expiry_Date IS DISTINCT FROM f_Long_Term_Agreement_Expiry_Date OR
#     i_Notice_Period IS DISTINCT FROM f_Notice_Period OR
#     i_Novated_Policy_Indicator IS DISTINCT FROM f_Novated_Policy_Indicator OR
#     i_Payment_Assignment_Policy_Indicator IS DISTINCT FROM f_Payment_Assignment_Policy_Indicator OR
#     i_Policy_Header_Reference IS DISTINCT FROM f_Policy_Header_Reference OR
#     i_Policy_Reference IS DISTINCT FROM f_Policy_Reference OR
#     i_Renewable_Indicator IS DISTINCT FROM f_Renewable_Indicator OR
#     i_Settlement_Frequency IS DISTINCT FROM f_Settlement_Frequency OR
#     i_Sub_Class_Code IS DISTINCT FROM f_Sub_Class_Code OR
#     i_Unrecognised_External_Policy_Indicator IS DISTINCT FROM f_Unrecognised_External_Policy_Indicator OR
#     i_Year_Of_Account IS DISTINCT FROM f_Year_Of_Account THEN Mismatch in Column Values
#             ELSE Exact Match
#         END AS Status
#     FROM CombinedData
# )
# SELECT * FROM Comparison;

# COMMAND ----------

# MAGIC %md
# MAGIC Sql implementation with dynamic columns

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CombinedData AS (
# MAGIC     SELECT 
# MAGIC         COALESCE(i.Policy_Section_Reference, f.Policy_Section_Reference) AS Policy_Section_Reference,
# MAGIC         CASE 
# MAGIC             WHEN i.Policy_Section_Reference IS NULL THEN 'Missing in Finact'
# MAGIC             WHEN f.Policy_Section_Reference IS NULL THEN 'Missing in Insights'
# MAGIC             WHEN HASH(i.*) <> HASH(f.*) THEN 'Mismatch in Column Values'
# MAGIC             ELSE 'Exact Match'
# MAGIC         END AS Status
# MAGIC     FROM insights_dev_003.finactdm.tbl_dim_policy_section i
# MAGIC     FULL OUTER JOIN insights_dev_003.dwh.dim_policy_section f
# MAGIC         ON i.Policy_Section_Reference = f.Policy_Section_Reference
# MAGIC )
# MAGIC SELECT * 
# MAGIC FROM CombinedData;
# MAGIC