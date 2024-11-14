# Open Charge Map 
# Case Study

## Overview

This project processes and analyzes electric vehicle (EV) charging data. It includes creating schemas, tables, and views to organize and query the data effectively. The project is divided into several notebooks, each performing specific tasks such as data ingestion, transformation, and analysis.

## Notebooks

### 1. Data Ingestion and Transformation

#### Description
This notebook handles the ingestion of raw data from various sources, including Azure Blob Storage and APIs. It performs necessary transformations to flatten nested JSON structures and enriches the data with additional information such as timezones.

#### Key Steps
- **Configuration**: Set up Azure Key Vault and ADLS Gen2 configurations.
- **Data Ingestion**: Load data from Azure Blob Storage and APIs.
- **Data Transformation**: Flatten nested JSON structures and add timezone information.
- **Data Enrichment**: Generate unique IDs for locations, EVSEs, and connectors.

#### Key Functions
- `list_json_files(folder_path)`: Recursively lists all JSON files in a given folder.
- `custom_flatten(df)`: Flattens nested JSON structures in a DataFrame.
- `get_timezone(lat, lon)`: Retrieves the timezone for given latitude and longitude.

### 2. Data Merging and Enrichment

#### Description
This notebook merges the transformed data into structured tables and enriches it with additional attributes. It ensures data consistency and integrity by generating unique IDs and merging data into existing tables.

#### Key Steps
- **Schema Creation**: Create schemas and tables if they do not exist.
- **Data Merging**: Merge transformed data into `charger_location`, `charger_evse`, and `charger_connector` tables.
- **Data Enrichment**: Generate unique IDs and add metadata columns.

#### Key Functions
- `create_source_evse_ids(uuid, num_points)`: Creates an array of source EVSE IDs.
- `create_source_connector_ids(source_evse_id, num_connections)`: Creates an array of source connector IDs.

### 3. Data Analysis and Reporting

#### Description
This notebook creates views to analyze and report on the EV charging data. It includes various KPIs and aggregated metrics to provide insights into the charging infrastructure.

#### Key Steps
- **View Creation**: Create views for charger counts by state, power category, and other metrics.
- **Data Aggregation**: Aggregate data to calculate KPIs and other metrics.
- **Reporting**: Generate reports to display the aggregated data.

#### Key Views
- `curated_emobility.us_charger_count_by_state`: Counts chargers by state and city.
- `curated_emobility.us_charger_count_by_power`: Counts chargers by power category.
- `curated_emobility.us_charger_count_by_power_details`: Detailed charger counts by power category, county, and operator.
- `curated_emobility.us_charging_stations_commissioned_over_time`: Counts charging stations commissioned over time.
- `curated_emobility.us_charging_kpis`: Calculates key performance indicators for charging stations.

## Setup

### Prerequisites
- Databricks environment
- Azure Blob Storage account
- Azure Key Vault
- Necessary Python libraries: `requests`, `json`, `pyspark`, `tzfpy`

### Configuration
1. **Azure Key Vault**: Store secrets for accessing Azure Blob Storage.
2. **ADLS Gen2**: Configure storage account and container for data storage.
3. **Databricks Secrets**: Store API keys and other sensitive information in Databricks secrets.

### Running the Notebooks
1. **Data Ingestion and Transformation**: Run the notebook to ingest and transform raw data.
2. **Data Merging and Enrichment**: Run the notebook to merge and enrich the transformed data.
3. **Data Analysis and Reporting**: Run the notebook to create views and generate reports.

## Usage

### Viewing Data
- Use the created views to query and analyze the EV charging data.
- Generate reports to gain insights into the charging stations management and network.

---
