# Databricks notebook source
import requests
import json
from pyspark.sql.functions import col, lit, current_timestamp, explode_outer, posexplode_outer
from pyspark.sql.types import StructType, ArrayType
from datetime import datetime

# COMMAND ----------

# Configuration for Azure Key Vault for storage account
kv_scope = "anu-access-scope"
key_vault_name = "de106kv50215"
key_vault_secret_name = "anu-storage-access"


# COMMAND ----------

# Fetch the secret from Azure Key Vault
storage_account_key = dbutils.secrets.get(scope=kv_scope, key=key_vault_secret_name)

# COMMAND ----------

# Configuration for ADLS Gen2
storage_account = "de10692367dl"
container_name = "anukansha"
mount_point = "/mnt/anukansha"

# COMMAND ----------

# Use the mount point to get path of source data
source_data_folder = f"{mount_point}/e-mob2/"
print(source_data_folder)

# COMMAND ----------

def list_json_files(folder_path):
    """
    Recursively lists all JSON files in the given folder path.

    Args:
        folder_path (str): The path to the folder to search for JSON files.

    Returns:
        list: A list of paths to JSON files.
    """
    files = dbutils.fs.ls(folder_path)
    json_files = []
    for file in files:
        if file.isDir():
            json_files.extend(list_json_files(file.path))
        elif file.path.endswith(".json"):
            json_files.append(file.path)
    return json_files

# COMMAND ----------

# List all JSON files in the source data folder and its subdirectories
json_files = list_json_files(source_data_folder)
print(json_files)

# COMMAND ----------

# Read the JSON files into a DataFrame and display it
df = spark.read.json(json_files)
display(df)

# COMMAND ----------

def custom_flatten(df):
    """
    Flattens nested JSON structures in a DataFrame.

    Args:
        df (DataFrame): The DataFrame to flatten.

    Returns:
        DataFrame: The flattened DataFrame.
    """
    complex_fields = {field.name: field.dataType for field in df.schema.fields if isinstance(field.dataType, (ArrayType, StructType))}
    
    while complex_fields:
        col_name = list(complex_fields.keys())[0]
        print(f"{col_name} Type: {type(complex_fields[col_name])}")
        
        if isinstance(complex_fields[col_name], StructType):
            expanded = [col(f"{col_name}.{k}").alias(f"{col_name}_{k}") for k in [n.name for n in complex_fields[col_name]]]
            df = df.select("*", *expanded).drop(col_name)
        
        elif isinstance(complex_fields[col_name], ArrayType):
            df = df.withColumn(col_name, explode_outer(col_name))
        
        complex_fields = {field.name: field.dataType for field in df.schema.fields if isinstance(field.dataType, (ArrayType, StructType))}
    
    return df

# COMMAND ----------

# Flatten the JSON structure using the function
flattened_df = custom_flatten(df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# COMMAND ----------

# Define window specification
windowSpec = Window.partitionBy("ID").orderBy("ID")

# Add Connections_num_of_points column
flattened_df = flattened_df.withColumn("Connections_num_of_points", row_number().over(windowSpec))

# COMMAND ----------

# Display the flattened dataframe
display(flattened_df)

# COMMAND ----------

# Add metadata columns
bronze_df = flattened_df.withColumn("source_name", lit("OCM_API")) \
                        .withColumn("created_timestamp", lit(datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))) \
                        .withColumn("ingest_timestamp", current_timestamp()) \
                        .withColumn("storage_path", lit(source_data_folder))

# COMMAND ----------

# Create the bronze layer schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS raw_emobility")

# COMMAND ----------

# Write to bronze table (Delta format)
bronze_df.write.format("delta").mode("overwrite").saveAsTable("raw_emobility.bronze_ocm_data")

print(f"Bronze table created in raw_emobility schema with data from {source_data_folder}")
