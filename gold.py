# Databricks notebook source
# Create the silver layer schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS curated_emobility")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW curated_emobility.us_charger_count_by_county AS
# MAGIC SELECT
# MAGIC     cl.county AS County,
# MAGIC     cl.city AS City,
# MAGIC     SUM(CASE WHEN cc.power_type IN ('AC (Single-Phase)', 'AC (Three-Phase)') THEN 1 ELSE 0 END) AS ac_charger_count,
# MAGIC     SUM(CASE WHEN cc.power_type = 'DC' THEN 1 ELSE 0 END) AS dc_charger_count,
# MAGIC     COUNT(*) AS total_charger_count
# MAGIC FROM
# MAGIC     euh_emobility.charger_location cl
# MAGIC JOIN
# MAGIC     euh_emobility.charger_connector cc ON cl.location_id = cc.evse_id
# MAGIC WHERE
# MAGIC     cl.country_code = 'US'
# MAGIC     AND cc.power_type IS NOT NULL
# MAGIC GROUP BY
# MAGIC     cl.county, cl.city;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM curated_emobility.us_charger_count_by_county;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW curated_emobility.us_charger_count_by_power AS
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN cc.power_kw <= 7 THEN 'Slow (<= 7 kW)'
# MAGIC         WHEN cc.power_kw <= 22 THEN 'Fast (> 7 kW and <= 22 kW)'
# MAGIC         WHEN cc.power_kw <= 50 THEN 'Rapid (> 22 kW and <= 50 kW)'
# MAGIC         ELSE 'Ultra-Rapid (> 50 kW)'
# MAGIC     END AS power_category,
# MAGIC     COUNT(*) AS charger_count
# MAGIC FROM
# MAGIC     euh_emobility.charger_connector cc
# MAGIC JOIN
# MAGIC     euh_emobility.charger_location cl ON cc.evse_id = cl.location_id
# MAGIC WHERE
# MAGIC     cl.country_code = 'US'
# MAGIC GROUP BY
# MAGIC     power_category;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM curated_emobility.us_charger_count_by_power;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW curated_emobility.us_charging_stations_commissioned_over_time AS
# MAGIC SELECT
# MAGIC     DATE_TRUNC('month', cl.commissioned) AS month,
# MAGIC     COUNT(*) AS charging_stations_count
# MAGIC FROM
# MAGIC     euh_emobility.charger_location cl
# MAGIC WHERE
# MAGIC     cl.country_code = 'US'
# MAGIC     AND cl.commissioned IS NOT NULL
# MAGIC GROUP BY
# MAGIC     month
# MAGIC ORDER BY
# MAGIC     month;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM curated_emobility.us_charging_stations_commissioned_over_time;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW curated_emobility.us_charging_kpis AS
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT cl.location_id) AS total_charging_stations,
# MAGIC     AVG(cc.power_kw) AS average_connector_power_kw,
# MAGIC     COUNT(DISTINCT cl.operator) AS unique_operators
# MAGIC FROM
# MAGIC     euh_emobility.charger_connector cc
# MAGIC JOIN
# MAGIC     euh_emobility.charger_location cl ON cc.evse_id = cl.location_id
# MAGIC WHERE
# MAGIC     cl.country_code = 'US';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM curated_emobility.us_charging_kpis;
