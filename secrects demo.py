# Databricks notebook source
# MAGIC %md
# MAGIC Creating secret scope using python API By Using PAT (Personal Access Token)

# COMMAND ----------

import requests

# Replace with your Databricks workspace URL and personal access token
workspace_url = "https://dbc-f450058a-abb8.cloud.databricks.com"
token = "dapib73019a6b2a604c2470e4ecd53853460"

# Define the secret scope name
scope_name = "test-secret-scope"

# API endpoint to create a secret scope
url = f"{workspace_url}/api/2.0/secrets/scopes/create"

# Headers and payload
headers = {"Authorization": f"Bearer {token}"}
payload = {"scope": scope_name}

# Make the POST request to create the secret scope
response = requests.post(url, headers=headers, json=payload)

# Check the response
if response.status_code == 200:
    print(f"Secret scope '{scope_name}' created successfully!")
else:
    print(f"Failed to create secret scope: {response.text}")


# COMMAND ----------

# MAGIC %md
# MAGIC Adding a secret to the scope by name and value

# COMMAND ----------

# API endpoint to add a secret
url = f"{workspace_url}/api/2.0/secrets/put"

# Define the secret key and value
key_name = "test-secret-key"
secret_value = "test-secret-value"

# Payload for adding the secret
payload = {
    "scope": scope_name,
    "key": key_name,
    "string_value": secret_value
}

# Make the POST request to add the secret
response = requests.post(url, headers=headers, json=payload)

# Check the response
if response.status_code == 200:
    print(f"Secret '{key_name}' added successfully to scope '{scope_name}'!")
else:
    print(f"Failed to add secret: {response.text}")


# COMMAND ----------

# MAGIC %md
# MAGIC Retrieving the saved the secret in the scope 

# COMMAND ----------

# Retrieve the secret
secret_value = dbutils.secrets.get(scope="test-secret-scope", key="test-secret-key")

# Use the secret in your code
print(f"The secret value is: {secret_value}")  # Avoid printing secrets in production


# COMMAND ----------

# MAGIC %md
# MAGIC Listing all secrets in the scope

# COMMAND ----------

# List all secrets in the scope
secrets = dbutils.secrets.list(scope="test-secret-scope")

# Print the secret keys
for secret in secrets:
    print(f"Secret Key: {secret.key}")


# COMMAND ----------

# MAGIC %md
# MAGIC Deleting the secret from the scope

# COMMAND ----------

# API endpoint to delete a secret
url = f"{workspace_url}/api/2.0/secrets/delete"

# Payload for deleting the secret
payload = {
    "scope": scope_name,
    "key": key_name
}

# Make the POST request to delete the secret
response = requests.post(url, headers=headers, json=payload)

# Check the response
if response.status_code == 200:
    print(f"Secret '{key_name}' deleted successfully from scope '{scope_name}'!")
else:
    print(f"Failed to delete secret: {response.text}")
