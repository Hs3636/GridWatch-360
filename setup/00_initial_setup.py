# Databricks notebook source
CATALOG = "gridwatch360"
SCHEMA  = "energy"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# Volumes for AutoLoader checkpoints and schema inference
for vol in ["checkpoints", "schema_store", "raw_uploads"]:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{vol}")

print("Catalog, schema, and volumes created.")

# COMMAND ----------

display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA}"))

# COMMAND ----------

# Save this as a widget-free config dict — imported via %run in every notebook

config = {
    # Namespaces
    "catalog":  "gridwatch360",
    "schema":   "energy",

    # Volume paths
    "checkpoint_base": "/Volumes/gridwatch360/energy/checkpoints",
    "schema_base":     "/Volumes/gridwatch360/energy/schema_store",

    # S3 source paths (AutoLoader reads from here)
    "s3_meter":    "s3://gridwatch-360-team5/meter_readings/",
    "s3_weather":  "s3://gridwatch-360-team5/weather_readings/",
    "s3_building": "s3://gridwatch-360-team5/building_metadata/",

    # Delta table FQNs
    "bronze_meter":    "gridwatch360.energy.bronze_meter_readings",
    "bronze_weather":  "gridwatch360.energy.bronze_weather_readings",
    "bronze_building": "gridwatch360.energy.bronze_building_metadata",

    "silver_readings": "gridwatch360.energy.silver_readings",
    "silver_weather":  "gridwatch360.energy.silver_weather",
    "silver_enriched": "gridwatch360.energy.silver_enriched_readings",

    "gold_fact_readings": "gridwatch360.energy.fact_meter_readings",
    "gold_fact_daily":    "gridwatch360.energy.fact_daily_building_summary",
    "gold_dim_date":      "gridwatch360.energy.dim_date",
    "gold_dim_building":  "gridwatch360.energy.dim_building",
    "gold_dim_site":      "gridwatch360.energy.dim_site",
    "gold_dim_meter":     "gridwatch360.energy.dim_meter_type",
    "gold_dim_time":      "gridwatch360.energy.dim_time_of_day",
    "gold_dim_use":       "gridwatch360.energy.dim_building_use",
    "gold_dim_weather":   "gridwatch360.energy.dim_weather_condition",

    # AWS (Gold export) — pull from env/secrets, never hardcode
    "s3_gold_bucket": "gridwatch-360-team5",
    "s3_gold_prefix": "gold/",
}

print("Config loaded:")
for k, v in config.items():
    print(f"  {k}: {v}")

# COMMAND ----------

