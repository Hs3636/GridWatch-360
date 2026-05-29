# Databricks notebook source
from datetime import datetime

LOG_TABLE = "gridwatch360.energy.silver_quality_log"
RUN_TIME  = datetime.utcnow().isoformat()
results   = []

def log_check(name, table, passed, details=""):
    results.append({"check_name": name, "table": table, "passed": passed,
                    "details": details, "checked_at": RUN_TIME})
    print(f"[{'PASS' if passed else 'FAIL'}] {name} | {details}")

# COMMAND ----------

df_r = spark.table("gridwatch360.energy.silver_readings")
df_w = spark.table("gridwatch360.energy.silver_weather")
df_e = spark.table("gridwatch360.energy.silver_enriched_readings")
df_b = spark.table("gridwatch360.energy.bronze_meter_readings")

# Readings
log_check("silver_vs_bronze_95pct",   "silver_readings", df_r.count() >= df_b.count() * 0.95,  f"silver={df_r.count():,} bronze={df_b.count():,}")
log_check("meter_label_not_null",     "silver_readings", df_r.filter("meter_label is null").count() == 0, "meter_label coverage")
log_check("all_4_meter_types",        "silver_readings", df_r.select("meter").distinct().count() == 4, "meter types 0-3 present")
log_check("reading_date_not_null",    "silver_readings", df_r.filter("reading_date is null").count() == 0, "reading_date null check")
log_check("no_negative_readings",     "silver_readings", df_r.filter("meter_reading < 0").count() == 0, "meter_reading >= 0")

# Weather
log_check("weather_no_null_airtemp",  "silver_weather",  df_w.filter("air_temperature is null").count() == 0, "air_temp null check")
log_check("all_temp_categories",      "silver_weather",  df_w.filter("temp_category is null").count() == 0, "temp_category coverage")
log_check("wind_direction_fix",       "silver_weather",  df_w.filter("wind_speed = 0 and wind_direction = 0").count() == 0, "wind 0/0 removed")

# Enriched
log_check("energy_per_sqft_positive", "silver_enriched", df_e.filter("energy_per_sqft < 0").count() == 0, "energy_per_sqft >= 0")
log_check("enriched_row_count",       "silver_enriched", df_e.count() > 0, f"rows={df_e.count():,}")

# COMMAND ----------

spark.createDataFrame(results).write.format("delta").mode("append").saveAsTable(LOG_TABLE)
print(f"Silver quality log written → {LOG_TABLE}")