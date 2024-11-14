# Databricks notebook source
# Create the silver layer schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS euh_emobility")


# COMMAND ----------

# Create charger_location table
spark.sql("""
CREATE TABLE IF NOT EXISTS euh_emobility.charger_location (
    location_id INT,
    operator_location_id STRING,
    source_location_id STRING,
    location_type STRING,
    location_sub_type STRING,
    name STRING,
    country_code STRING,
    address STRING,
    city STRING,
    county STRING,
    postal_code STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    timezone STRING,
    status STRING,
    operator STRING,
    owning_company STRING,
    source STRING,
    created TIMESTAMP,
    modified TIMESTAMP,
    processed TIMESTAMP,
    commissioned DATE,
    decommissioned DATE
)
""")

# COMMAND ----------

# Create charger_evse table
spark.sql("""
CREATE TABLE IF NOT EXISTS euh_emobility.charger_evse (
    evse_id INT,
    location_id INT,
    source STRING,
    source_evse_id STRING,
    operator_evse_id STRING,
    ocpi_evse_id STRING,
    chargepoint_id STRING,
    manufacturer STRING,
    model STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    created TIMESTAMP,
    modified TIMESTAMP,
    processed TIMESTAMP,
    commissioned DATE,
    decommissioned DATE
)
""")

# COMMAND ----------

# Create charger_connector table
spark.sql("""
CREATE TABLE IF NOT EXISTS euh_emobility.charger_connector (
    connector_id INT,
    evse_id INT,
    source STRING,
    source_connector_id STRING,
    operator_connector_id STRING,
    ocpi_connector_id STRING,
    connector_type STRING,
    power_type STRING,
    phase INT,
    voltage INT,
    amperage INT,
    power_kw DECIMAL(7, 2),
    created TIMESTAMP,
    modified TIMESTAMP,
    processed TIMESTAMP
)
""")

# COMMAND ----------

from tzfpy import get_tz
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def get_timezone(lat, lon):
    try:
        timezone = get_tz(lat, lon)
        return timezone
    except Exception as e:
        return 'None'  # Returning None if there is an error

# Register the function as a UDF
get_timezone_udf = udf(lambda lat, lon: get_timezone(lat, lon), StringType())
spark.udf.register("get_timezone", get_timezone_udf)


# COMMAND ----------

# Load your source DataFrame
source_df = spark.sql("SELECT * FROM raw_emobility.bronze_ocm_data")

# Apply the UDF to add the timezone column
source_df = source_df.withColumn("timezone", get_timezone_udf(source_df["AddressInfo_Latitude"], source_df["AddressInfo_Longitude"]))

# Create a temporary view for the updated DataFrame
source_df.createOrReplaceTempView("updated_anu_ocm_bronze")


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Generate new location_id values starting from 1
window_spec = Window.orderBy("AddressInfo_ID")
source_df = source_df.withColumn("location_id", row_number().over(window_spec))

# Create a temporary view for the updated DataFrame with new location_id values
source_df.createOrReplaceTempView("updated_anu_ocm_bronze_with_ids")


# COMMAND ----------

spark.sql("""
MERGE INTO euh_emobility.charger_location AS target
USING (
    SELECT DISTINCT
        location_id,
        NULL AS operator_location_id,
        UUID AS source_location_id,
        'PUBLIC' AS location_type,
        NULL AS location_sub_type,
        AddressInfo_AddressLine1 AS name,  -- Duplicate of the first line of the address
        AddressInfo_Country_ISOCode AS country_code,
        CONCAT(AddressInfo_AddressLine1, ' ', AddressInfo_AddressLine2) AS address,
        AddressInfo_Town AS city,
        AddressInfo_StateOrProvince AS county,
        AddressInfo_Postcode AS postal_code,
        AddressInfo_Latitude AS Latitude,
        AddressInfo_Longitude AS Longitude,
        timezone,
        CASE WHEN StatusType_Title = 'Planned For Future Date' THEN 'PLANNED' ELSE 'ACTIVE' END AS status,
        OperatorInfo_Title AS operator,
        OperatorInfo_Title AS owning_company,
        'OCM_API' AS source,
        created_timestamp AS created,
        current_timestamp() AS modified,
        current_timestamp() AS processed,
        DatePlanned AS commissioned,
        NULL AS decommissioned
    FROM updated_anu_ocm_bronze_with_ids
) AS source
ON target.location_id = source.location_id
WHEN MATCHED THEN
    UPDATE SET
        target.operator_location_id = source.operator_location_id,
        target.source_location_id = source.source_location_id,
        target.location_type = source.location_type,
        target.location_sub_type = source.location_sub_type,
        target.name = source.name,
        target.country_code = source.country_code,
        target.address = source.address,
        target.city = source.city,
        target.county = source.county,
        target.postal_code = source.postal_code,
        target.latitude = source.latitude,
        target.longitude = source.longitude,
        target.timezone = source.timezone,
        target.status = source.status,
        target.operator = source.operator,
        target.owning_company = source.owning_company,
        target.source = source.source,
        target.created = source.created,
        target.modified = source.modified,
        target.processed = source.processed,
        target.commissioned = source.commissioned,
        target.decommissioned = source.decommissioned
WHEN NOT MATCHED THEN
    INSERT (location_id, operator_location_id, source_location_id, location_type, location_sub_type, name, country_code, address, city, county, postal_code, latitude, longitude, timezone, status, operator, owning_company, source, created, modified, processed, commissioned, decommissioned)
    VALUES (source.location_id, source.operator_location_id, source.source_location_id, source.location_type, source.location_sub_type, source.name, source.country_code, source.address, source.city, source.county, source.postal_code, source.latitude, source.longitude, source.timezone, source.status, source.operator, source.owning_company, source.source, source.created, source.modified, source.processed, source.commissioned, source.decommissioned)
""")


# COMMAND ----------

# Display the contents of the charger_location table
charger_location_df = spark.sql("SELECT * FROM euh_emobility.charger_location")
display(charger_location_df)

# COMMAND ----------

from pyspark.sql.functions import explode, array, lit
from pyspark.sql.types import ArrayType

# COMMAND ----------

# Define a function to create an array of source_evse_id values
def create_source_evse_ids(uuid, num_points):
    if num_points is None:
        num_points = 1
    return [f"{uuid}-{i+1}" for i in range(num_points)]

# COMMAND ----------

# Register the function as a UDF
create_source_evse_ids_udf = udf(create_source_evse_ids, ArrayType(StringType()))
spark.udf.register("create_source_evse_ids", create_source_evse_ids_udf)

# COMMAND ----------

# Apply the UDF to create an array of source_evse_id values
source_df = source_df.withColumn("source_evse_ids", create_source_evse_ids_udf(source_df["UUID"], source_df["NumberOfPoints"]))

# Explode the array to create multiple rows
source_df = source_df.withColumn("source_evse_id", explode(source_df["source_evse_ids"]))

# Generate new evse_id values starting from 1
window_spec = Window.orderBy("ID")
source_df = source_df.withColumn("evse_id", row_number().over(window_spec))

# Create a temporary view for the updated DataFrame with new evse_id values
source_df.createOrReplaceTempView("updated_anu_ocm_bronze_with_evse_ids")

# COMMAND ----------

display(source_df)

# COMMAND ----------

# Merge data into charger_evse
spark.sql("""
MERGE INTO euh_emobility.charger_evse AS target
USING (
    SELECT DISTINCT
        evse_id,
        location_id,
        'OCM_API' AS source,
        source_evse_id,
        NULL AS operator_evse_id,
        NULL AS ocpi_evse_id,
        NULL AS chargepoint_id,
        NULL AS manufacturer,
        NULL AS model,
        AddressInfo_Latitude AS latitude,
        AddressInfo_Longitude AS longitude,
        created_timestamp AS created,
        current_timestamp() AS modified,
        current_timestamp() AS processed,
        NULL AS commissioned,
        NULL AS decommissioned
    FROM updated_anu_ocm_bronze_with_evse_ids
) AS source
ON target.evse_id = source.evse_id
WHEN MATCHED THEN
    UPDATE SET
        target.location_id = source.location_id,
        target.source = source.source,
        target.source_evse_id = source.source_evse_id,
        target.operator_evse_id = source.operator_evse_id,
        target.ocpi_evse_id = source.ocpi_evse_id,
        target.chargepoint_id = source.chargepoint_id,
        target.manufacturer = source.manufacturer,
        target.model = source.model,
        target.latitude = source.latitude,
        target.longitude = source.longitude,
        target.created = source.created,
        target.modified = source.modified,
        target.processed = source.processed,
        target.commissioned = source.commissioned,
        target.decommissioned = source.decommissioned
WHEN NOT MATCHED THEN
    INSERT (evse_id, location_id, source, source_evse_id, operator_evse_id, ocpi_evse_id, chargepoint_id, manufacturer, model, latitude, longitude, created, modified, processed, commissioned, decommissioned)
    VALUES (source.evse_id, source.location_id, source.source, source.source_evse_id, source.operator_evse_id, source.ocpi_evse_id, source.chargepoint_id, source.manufacturer, source.model, source.latitude, source.longitude, source.created, source.modified, source.processed, source.commissioned, source.decommissioned)
""")

# COMMAND ----------

# Display the contents of the charger_evse table
charger_evse_df = spark.sql("SELECT * FROM euh_emobility.charger_evse")
display(charger_evse_df)

# COMMAND ----------

# Using DELETE
spark.sql("DELETE FROM euh_emobility.charger_connector")


# COMMAND ----------

# Define a function to create an array of source_connector_id values
def create_source_connector_ids(source_evse_id, num_connections):
    return [f"{source_evse_id}-{num_connections}"]

# COMMAND ----------

# Register the function as a UDF
create_source_connector_ids_udf = udf(create_source_connector_ids, ArrayType(StringType()))
spark.udf.register("create_source_connector_ids", create_source_connector_ids_udf)

# COMMAND ----------

# Apply the UDF to create an array of source_connector_id values
source_df = source_df.withColumn("source_connector_ids", create_source_connector_ids_udf(source_df["UUID"], source_df["Connections_num_of_points"]))

# COMMAND ----------

# Explode the array to create multiple rows
source_df = source_df.withColumn("source_connector_id", explode(source_df["source_connector_ids"]))

# Generate new connector_id values starting from 1
window_spec = Window.orderBy("Connections_ID")
source_df = source_df.withColumn("connector_id", row_number().over(window_spec))

# Create a temporary view for the updated DataFrame with new connector_id values
source_df.createOrReplaceTempView("updated_anu_ocm_bronze_with_connector_ids")

# COMMAND ----------

display(source_df)

# COMMAND ----------

# Merge data into charger_connector
spark.sql("""
MERGE INTO euh_emobility.charger_connector AS target
USING (
    SELECT DISTINCT
        Connections_ID AS connector_id,
        evse_id AS evse_id,
        'OCM_API' AS source,
        source_connector_id AS source_connector_id,
        NULL AS operator_connector_id,
        NULL AS ocpi_connector_id,
        Connections_ConnectionType_Title AS connector_type,
        Connections_CurrentType_Title AS power_type,
        NULL AS phase,
        Connections_Voltage AS voltage,
        Connections_Amps AS amperage,
        Connections_PowerKW AS power_kw,
        created_timestamp AS created,
        current_timestamp() AS modified,
        current_timestamp() AS processed
    FROM updated_anu_ocm_bronze_with_connector_ids
) AS source
ON target.connector_id = source.connector_id
WHEN MATCHED THEN
    UPDATE SET
        target.evse_id = source.evse_id,
        target.source = source.source,
        target.source_connector_id = source.source_connector_id,
        target.operator_connector_id = source.operator_connector_id,
        target.ocpi_connector_id = source.ocpi_connector_id,
        target.connector_type = source.connector_type,
        target.power_type = source.power_type,
        target.phase = source.phase,
        target.voltage = source.voltage,
        target.amperage = source.amperage,
        target.power_kw = source.power_kw,
        target.created = source.created,
        target.modified = source.modified,
        target.processed = source.processed
WHEN NOT MATCHED THEN
    INSERT (connector_id, evse_id, source, source_connector_id, operator_connector_id, ocpi_connector_id, connector_type, power_type, phase, voltage, amperage, power_kw, created, modified, processed)
    VALUES (source.connector_id, source.evse_id, source.source, source.source_connector_id, source.operator_connector_id, source.ocpi_connector_id, source.connector_type, source.power_type, source.phase, source.voltage, source.amperage, source.power_kw, source.created, source.modified, source.processed)
""")

# COMMAND ----------

# Display the contents of the charger_connector table
charger_connector_df = spark.sql("SELECT * FROM euh_emobility.charger_connector")
display(charger_connector_df)