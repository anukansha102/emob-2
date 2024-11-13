# Databricks notebook source
import requests
import json
from azure.storage.blob import BlobServiceClient

# COMMAND ----------

# Configuration for Azure Key Vault for storage account
kv_scope = "anu-access-scope"
key_vault_name = "de106kv50215"
key_vault_secret_name = "anu-storage-access"
# Fetch the secret from Azure Key Vault
storage_account_key = dbutils.secrets.get(scope=kv_scope, key=key_vault_secret_name)

# COMMAND ----------

# Configuration for ADLS Gen2
storage_account = "de10692367dl"
container_name = "anukansha"
mount_point = "/mnt/anukansha"

# COMMAND ----------

# Mount the storage account using the wasbs scheme
dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account}.blob.core.windows.net/",
  mount_point = mount_point,
  extra_configs = {f"fs.azure.account.key.{storage_account}.blob.core.windows.net": storage_account_key}
)

# COMMAND ----------

# Use the mount point in your data path
source_data_path = f"{mount_point}/e-mob2/source-data/"
print(source_data_path)

# COMMAND ----------

# Fetch the API key from Databricks secrets
api_key = dbutils.secrets.get(scope="anu-emob-2", key="anu-api-key")

# COMMAND ----------

url = "https://api.openchargemap.io/v3/poi/"
headers = {
    "X-API-Key": api_key
}
params = {
    "output": "json",
    "countrycode": "US",
    "maxresults": 100000
}


# COMMAND ----------

# Make the API request
response = requests.get(url, headers=headers, params=params)


# COMMAND ----------

if response.status_code == 200:
    data = response.json()

# COMMAND ----------

# Convert the data to a Spark DataFrame
df = spark.read.json(spark.sparkContext.parallelize([json.dumps(data)]))

# COMMAND ----------

# Show the DataFrame schema and data
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

from datetime import datetime

def time_stamp():
    return datetime.now().strftime("%Y-%m-%dT%H_%M_%SZ")

# COMMAND ----------

file_path = source_data_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/e-mob2/source-data/OCM_data_{time_stamp()}"

# COMMAND ----------

# Coalesce the DataFrame to a single partition and write to JSON format in Azure Blob Storage
df.coalesce(1).write.json(source_data_path, mode="overwrite")
print("Data written to Azure Blob Storage in JSON format")
