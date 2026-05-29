# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime

LOG_TABLE   = "gridwatch360.energy.bronze_ingestion_log"
RUN_TIME    = datetime.utcnow().isoformat()
results     = []

# COMMAND ----------

def log_check(check_name, table, passed, details=""):
    results.append({
        "check_name":  check_name,
        "table":       table,
        "passed":      passed,
        "details":     details,
        "checked_at":  RUN_TIME
    })
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {check_name} | {details}")

# --- Meter readings ---
df_meter = spark.table("gridwatch360.energy.bronze_meter_readings")
meter_count = df_meter.count()
log_check("meter_row_count_gt_0",    "bronze_meter_readings",   meter_count > 0,        f"rows={meter_count:,}")
log_check("meter_building_id_notnull","bronze_meter_readings",  df_meter.filter("building_id is null").count() == 0, "building_id null check")
log_check("meter_reading_notnull",   "bronze_meter_readings",   df_meter.filter("meter_reading is null").count() == 0, "meter_reading null check")
log_check("meter_types_valid",       "bronze_meter_readings",   df_meter.filter("meter not in (0,1,2,3)").count() == 0, "meter type range 0-3")

# --- Weather ---
df_weather = spark.table("gridwatch360.energy.bronze_weather_readings")
weather_count = df_weather.count()
log_check("weather_row_count_gt_0",  "bronze_weather_readings", weather_count > 0,      f"rows={weather_count:,}")
log_check("weather_site_id_notnull", "bronze_weather_readings", df_weather.filter("site_id is null").count() == 0, "site_id null check")
log_check("weather_timestamp_notnull","bronze_weather_readings",df_weather.filter("timestamp is null").count() == 0, "timestamp null check")

# --- Building metadata ---
df_building = spark.table("gridwatch360.energy.bronze_building_metadata")
building_count = df_building.filter("is_current = true").count()
log_check("building_row_count_gt_0", "bronze_building_metadata",building_count > 0,     f"current rows={building_count}")
log_check("building_id_notnull",     "bronze_building_metadata",df_building.filter("building_id is null").count() == 0, "building_id null check")
log_check("building_scd2_no_dup_current", "bronze_building_metadata",
          df_building.filter("is_current=true").groupBy("building_id").count().filter("count > 1").count() == 0,
          "no duplicate current building_id")

# COMMAND ----------

log_df = spark.createDataFrame(results)
log_df.write.format("delta").mode("append").saveAsTable(LOG_TABLE)
print(f"\nQuality log written → {LOG_TABLE}")