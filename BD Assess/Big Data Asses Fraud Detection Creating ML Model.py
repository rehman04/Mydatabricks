# Databricks notebook source
# MAGIC %pip install databricks-sdk==0.36.0 mlflow==2.19.0 databricks-feature-store==0.17.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   is_fraud,
# MAGIC   count(1) as `Transactions`, 
# MAGIC   sum(amount) as `Total Amount` 
# MAGIC from asscom1fraudanalysis.bdcom1asses.gold_transactions
# MAGIC group by is_fraud
# MAGIC

# COMMAND ----------

from plotly.subplots import make_subplots
import plotly.graph_objects as go

df = spark.sql(
    'select type, is_fraud, count(1) as count from asscom1fraudanalysis.bdcom1asses.gold_transactions group by type, is_fraud'
).toPandas()

fig = make_subplots(rows=1, cols=2, specs=[[{'type':'domain'}, {'type':'domain'}]])
fig.add_trace(go.Pie(labels=df[df['is_fraud']]['type'], values=df[df['is_fraud']]['count'], title="Fraud Transactions", hole=0.6), 1, 1)
fig.add_trace(go.Pie(labels=df[~df['is_fraud']]['type'], values=df[~df['is_fraud']]['count'], title="Normal Transactions", hole=0.6), 1, 2)

fig.show()

# COMMAND ----------

# Convert to koalas
dataset = spark.table('asscom1fraudanalysis.bdcom1asses.gold_transactions').dropDuplicates(['id']).pandas_api()
# Drop columns we don't want to use in our model
# Typical DS project would include more transformations / cleanup here
dataset = dataset.drop(columns=['address', 'email', 'firstname', 'lastname', 'creation_date', 'last_activity_date', 'customer_id'])

# Drop missing values
dataset.dropna()
dataset.describe()

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

try:
  #drop table if exists
  fs.drop_table('asscom1fraudanalysis.bdcom1asses.transactions_features')
except:
  pass

fs.create_table(
  name='asscom1fraudanalysis.bdcom1asses.transactions_features',
  primary_keys='id',
  schema=dataset.spark.schema(),
  description='These features are derived from the gold_transactions table in the lakehouse. created dummy variables for the categorical columns, cleaned up their names, and added a boolean flag for whether the transaction is a fraud or not.  No aggregations were performed.')

fs.write_table(df=dataset.to_spark(), name='asscom1fraudanalysis.bdcom1asses.transactions_features', mode='overwrite')
features = fs.read_table('asscom1fraudanalysis.bdcom1asses.transactions_features')
display(features)

# COMMAND ----------

# MAGIC %pip install databricks-automl-runtime

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()
features = fs.read_table('asscom1fraudanalysis.bdcom1asses.transactions_features')

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()
features = fs.read_table('asscom1fraudanalysis.bdcom1asses.transactions_features')

# Random 25% sample
quarter_sample = features.sample(withReplacement=False, fraction=0.002, seed=42)

print(f"Original count: {features.count()}")
print(f"25% sample count: {quarter_sample.count()}")

# COMMAND ----------

features = quarter_sample

# COMMAND ----------

from databricks import automl
# from databricks.automl import classify
from datetime import datetime

# Define your experiment path (customize this!)
xp_path = "/Shared/experiments/my_fraud_project_shifa"  # Or your user directory
xp_name = f"automl_fraud_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}"  # Unique name

# Run AutoML
automl_run = automl.classify(
    experiment_name=xp_name,
    experiment_dir=xp_path,
    dataset=features,  # Your feature DataFrame (no sampling needed if not a demo)
    target_col="is_fraud",  # Your target column
    timeout_minutes=30  # Adjust as needed
)



# COMMAND ----------

# Get the best trial's MLflow run ID
best_run_id = automl_run.best_trial.mlflow_run_id

# Load the best model as a PySpark/Python UDF
import mlflow
best_model = mlflow.pyfunc.spark_udf(
    spark, 
    model_uri=f"runs:/{best_run_id}/model"
)

# Display metrics
display(automl_run.best_trial)

# COMMAND ----------

# Or show in notebook:
display(automl_run)

# COMMAND ----------

# MAGIC %md
# MAGIC Journey to register the model in the catalog

# COMMAND ----------

# Step 0: Set MLflow to Unity Catalog *FIRST*
import mlflow
mlflow.set_registry_uri('databricks-uc')  # Important to set this first!

# COMMAND ----------

# Step 2: Register the best model into Unity Catalog
best_trial = automl_run.best_trial
best_model_path = best_trial.model_path

catalog = "asscom1fraudanalysis"    
db = "bdcom1asses"         
registered_model_name = "fraud_detection_shifa"  # Keep this name

# Full model name includes catalog and schema/database
full_model_name = f"{catalog}.{db}.{registered_model_name}"

model_registered = mlflow.register_model(
    model_uri=best_model_path,
    name=full_model_name  # Directly register into UC
)



# COMMAND ----------

# MAGIC %md
# MAGIC Making Predictions

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient
import pyspark.sql.functions as F
import mlflow
import pandas as pd
# 1. Load 10 random samples
fs = FeatureStoreClient()
features = fs.read_table('asscom1fraudanalysis.bdcom1asses.transactions_features')
random_10_samples = features.orderBy(F.rand(seed=42)).limit(10).toPandas()
# 2. Load model and get its schema
model_uri = f"models:/{full_model_name}/{model_registered.version}"
model = mlflow.pyfunc.load_model(model_uri)
input_schema = model.metadata.get_input_schema()
# 3. Schema Alignment
required_columns = input_schema.input_names()
required_dtypes = {field.name: field.type for field in input_schema.inputs}
# Map model schema types to pandas types
dtype_mapping = {'integer': 'int64','long': 'int64','float': 'float64','double': 'float64','string': 'object','boolean': 'bool'}
# Create DataFrame with correct columns and dtypes
model_ready_samples = pd.DataFrame(columns=required_columns)
# Populate with available data (maintaining order and types)
for col in required_columns:
    if col in random_10_samples.columns:
        pandas_dtype = dtype_mapping.get(required_dtypes[col], 'object')
        model_ready_samples[col] = random_10_samples[col].astype(pandas_dtype)
    else:
        # Handle missing columns (fill with defaults or raise error)
        print(f"Warning: Missing required column {col}")
        model_ready_samples[col] = 0  # Or appropriate default
# 4. Get predictions
predictions = model.predict(model_ready_samples)
# 5. Display results
results = model_ready_samples.copy()
results['prediction'] = predictions
display(results)