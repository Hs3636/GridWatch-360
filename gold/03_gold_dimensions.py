# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable
import uuid
from datetime import datetime, date

CATALOG  = "gridwatch360"
SCHEMA   = "energy"
ENRICHED = "gridwatch360.energy.silver_enriched_readings"
WEATHER  = "gridwatch360.energy.silver_weather"
BUILDING = "gridwatch360.energy.bronze_building_metadata"
BATCH_ID = str(uuid.uuid4())
RUN_TS   = datetime.utcnow().isoformat()

print(f"Batch ID : {BATCH_ID}")
print(f"Run time : {RUN_TS}")

# COMMAND ----------

TARGET = "gridwatch360.energy.dim_date"

date_df = spark.sql("""
    SELECT
        CAST(date_format(date, 'yyyyMMdd') AS INT)  AS date_key,
        date,
        year(date)                                  AS year,
        month(date)                                 AS month,
        dayofmonth(date)                            AS day,
        quarter(date)                               AS quarter,
        dayofweek(date)                             AS day_of_week,
        date_format(date, 'EEEE')                   AS day_name,
        date_format(date, 'MMMM')                   AS month_name,
        CASE WHEN dayofweek(date) IN (1,7)
             THEN true ELSE false END               AS is_weekend,
        weekofyear(date)                            AS week_of_year
    FROM (
        SELECT explode(
            sequence(to_date('2010-01-01'), to_date('2025-12-31'), interval 1 day)
        ) AS date
    )
""")

if not spark.catalog.tableExists(TARGET):
    date_df.write.format("delta").saveAsTable(TARGET)
    print(f"DIM_DATE created: {date_df.count():,} rows")
else:
    print("DIM_DATE already exists — skipping.")

# COMMAND ----------

TARGET = "gridwatch360.energy.dim_meter_type"

data = [
    (0, "Electricity",   "kWh"),
    (1, "Chilled Water", "kBTU"),
    (2, "Steam",         "kBTU"),
    (3, "Hot Water",     "kBTU"),
]

dim_meter = spark.createDataFrame(data, ["meter_type_key", "meter_label", "unit"])

if not spark.catalog.tableExists(TARGET):
    dim_meter.write.format("delta").saveAsTable(TARGET)
    print("DIM_METER_TYPE created.")
else:
    print("DIM_METER_TYPE already exists — skipping.")

# COMMAND ----------

TARGET = "gridwatch360.energy.dim_time_of_day"

dim_time = spark.sql("""
    SELECT
        hour                                        AS time_of_day_key,
        hour                                        AS hour,
        CASE
            WHEN hour BETWEEN 0  AND 5  THEN 'Night'
            WHEN hour BETWEEN 6  AND 11 THEN 'Morning'
            WHEN hour BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN hour BETWEEN 18 AND 23 THEN 'Evening'
        END                                         AS period_label,
        CASE WHEN hour BETWEEN 9 AND 17
             THEN true ELSE false END               AS is_business_hour
    FROM (
        SELECT explode(sequence(0, 23)) AS hour
    )
""")

if not spark.catalog.tableExists(TARGET):
    dim_time.write.format("delta").saveAsTable(TARGET)
    print("DIM_TIME_OF_DAY created.")
else:
    print("DIM_TIME_OF_DAY already exists — skipping.")

# COMMAND ----------

TARGET = "gridwatch360.energy.dim_site"

dim_site = spark.sql(f"""
    SELECT DISTINCT
        site_id                                         AS site_key,
        site_id,
        concat('Site_', lpad(cast(site_id as string), 2, '0')) AS site_name
    FROM {ENRICHED}
    WHERE site_id IS NOT NULL
    ORDER BY site_id
""")

if not spark.catalog.tableExists(TARGET):
    dim_site.write.format("delta").saveAsTable(TARGET)
    print(f"DIM_SITE created: {dim_site.count()} rows")
else:
    # Type 1 — overwrite on reload
    dim_site.write.format("delta").mode("overwrite").saveAsTable(TARGET)
    print(f"DIM_SITE refreshed: {dim_site.count()} rows")

# COMMAND ----------

TARGET = "gridwatch360.energy.dim_building_use"

dim_use = spark.sql(f"""
    SELECT DISTINCT
        dense_rank() OVER (ORDER BY primary_use)    AS use_key,
        primary_use
    FROM {BUILDING}
    WHERE primary_use IS NOT NULL
    ORDER BY primary_use
""")

if not spark.catalog.tableExists(TARGET):
    dim_use.write.format("delta").saveAsTable(TARGET)
    print(f"DIM_BUILDING_USE created: {dim_use.count()} rows")
else:
    dim_use.write.format("delta").mode("overwrite").saveAsTable(TARGET)
    print(f"DIM_BUILDING_USE refreshed: {dim_use.count()} rows")

# COMMAND ----------

TARGET = "gridwatch360.energy.dim_building"

# Source: bronze_building_metadata already has SCD2 structure (effective_from, effective_to, is_current)
df_src = (
    spark.table(BUILDING)
         .select(
             "building_id", "site_id", "primary_use", "square_feet",
             "year_built", "floor_count", "effective_from", "effective_to", "is_current"
         )
         .withColumn(
             "building_age",
             F.when(F.col("year_built").isNotNull(), F.lit(2016) - F.col("year_built"))
              .otherwise(F.lit(None).cast("int"))
         )
         .withColumn("is_year_unknown",  F.col("year_built").isNull())
         .withColumn("is_floor_unknown", F.col("floor_count").isNull())
         .withColumn("floor_count",      F.coalesce(F.col("floor_count"), F.lit(0)))
)

# Surrogate key = hash of business key + effective_from (stable, reproducible)
df_dim = df_src.withColumn(
    "building_key",
    F.sha2(
        F.concat_ws("||",
            F.col("building_id").cast("string"),
            F.col("effective_from").cast("string")
        ), 256
    )
).select(
    "building_key", "building_id", "site_id", "primary_use",
    "square_feet", "year_built", "floor_count", "building_age",
    "is_year_unknown", "is_floor_unknown",
    "effective_from", "effective_to", "is_current"
)

if not spark.catalog.tableExists(TARGET):
    df_dim.write.format("delta").saveAsTable(TARGET)
    print(f"DIM_BUILDING created: {df_dim.count()} rows")
else:
    # Merge — keep existing SCD2 history, insert only new keys
    target_dt = DeltaTable.forName(spark, TARGET)
    target_dt.alias("tgt").merge(
        df_dim.alias("src"),
        "tgt.building_key = src.building_key"
    ).whenNotMatchedInsertAll().execute()
    print(f"DIM_BUILDING merged: {spark.table(TARGET).count()} total rows")

# COMMAND ----------

TARGET = "gridwatch360.energy.dim_weather_condition"

dim_weather = (
    spark.table(WEATHER)
         .select(
             "site_id", "weather_date", "weather_hour",
             "air_temperature", "dew_temperature", "wind_speed",
             "wind_direction", "cloud_coverage", "sea_level_pressure",
             "precip_depth_1_hr", "temp_category"
         )
         # surrogate key for join in facts
         .withColumn(
             "weather_key",
             F.sha2(
                 F.concat_ws("||",
                     F.col("site_id").cast("string"),
                     F.col("weather_date").cast("string"),
                     F.col("weather_hour").cast("string")
                 ), 256
             )
         )
)

if not spark.catalog.tableExists(TARGET):
    dim_weather.write.format("delta").saveAsTable(TARGET)
    print(f"DIM_WEATHER_CONDITION created: {dim_weather.count():,} rows")
else:
    dim_weather.write.format("delta").mode("overwrite").saveAsTable(TARGET)
    print(f"DIM_WEATHER_CONDITION refreshed: {dim_weather.count():,} rows")

# COMMAND ----------

dims = [
    "dim_date", "dim_meter_type", "dim_time_of_day", "dim_site",
    "dim_building_use", "dim_building", "dim_weather_condition"
]

print(f"{'Table':<30} {'Row Count':>12}")
print("-" * 45)
for d in dims:
    cnt = spark.table(f"gridwatch360.energy.{d}").count()
    print(f"{d:<30} {cnt:>12,}")