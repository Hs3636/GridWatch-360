# Databricks notebook source
from datetime import datetime

LOG_TABLE = "gridwatch360.energy.gold_quality_log"
RUN_TIME  = datetime.utcnow().isoformat()
results   = []

def log_check(name, table, passed, details=""):
    results.append({"check_name": name, "table": table, "passed": passed,
                    "details": details, "checked_at": RUN_TIME})
    print(f"[{'PASS' if passed else 'FAIL'}] {name} | {details}")

# COMMAND ----------

from pyspark.sql import functions as F

df_fact   = spark.table("gridwatch360.energy.fact_meter_readings")
df_daily  = spark.table("gridwatch360.energy.fact_daily_building_summary")
df_ddate  = spark.table("gridwatch360.energy.dim_date")
df_dbldg  = spark.table("gridwatch360.energy.dim_building")
df_dsite  = spark.table("gridwatch360.energy.dim_site")
df_dmeter = spark.table("gridwatch360.energy.dim_meter_type")
df_dtime  = spark.table("gridwatch360.energy.dim_time_of_day")

# --- Completeness ---
log_check("fact_building_key_not_null",   "fact_meter_readings",
          df_fact.filter("building_key is null").count() == 0, "building_key null check")
log_check("fact_date_key_not_null",       "fact_meter_readings",
          df_fact.filter("date_key is null").count() == 0, "date_key null check")
log_check("fact_meter_reading_not_null",  "fact_meter_readings",
          df_fact.filter("meter_reading is null").count() == 0, "meter_reading null check")
log_check("daily_building_key_not_null",  "fact_daily_building_summary",
          df_daily.filter("building_key is null").count() == 0, "building_key null check")
log_check("dim_building_use_not_null",    "dim_building",
          df_dbldg.filter("primary_use is null").count() == 0, "primary_use null check")

# --- Uniqueness ---
log_check("fact_reading_key_unique",      "fact_meter_readings",
          df_fact.count() == df_fact.select("reading_key").distinct().count(),
          "reading_key uniqueness")
log_check("daily_building_date_key_unique","fact_daily_building_summary",
          df_daily.count() == df_daily.select("building_date_key").distinct().count(),
          "building_date_key uniqueness")
log_check("dim_building_no_dup_current",  "dim_building",
          df_dbldg.filter("is_current=true").groupBy("building_id")
                  .count().filter("count > 1").count() == 0,
          "no duplicate current building_id")

# --- Accuracy ---
log_check("daily_total_energy_correct",  "fact_daily_building_summary",
          df_daily.filter(
              F.round(F.col("total_energy"), 2) !=
              F.round(F.col("total_electricity_kwh") + F.col("total_chilled_water") +
                      F.col("total_steam") + F.col("total_hot_water"), 2)
          ).count() == 0, "total_energy = sum of 4 meters")
log_check("completeness_pct_range",      "fact_daily_building_summary",
          df_daily.filter("data_completeness_pct < 0 or data_completeness_pct > 100")
                  .count() == 0, "completeness 0-100%")
log_check("hours_with_data_range",       "fact_daily_building_summary",
          df_daily.filter("hours_with_data < 0 or hours_with_data > 24")
                  .count() == 0, "hours 0-24")

# --- Consistency ---
log_check("all_16_sites_present",        "dim_site",
          df_dsite.count() == 16, f"site count={df_dsite.count()}")
log_check("all_4_meter_types_in_dim",    "dim_meter_type",
          df_dmeter.count() == 4, f"meter type count={df_dmeter.count()}")
log_check("dim_time_24_rows",            "dim_time_of_day",
          df_dtime.count() == 24, f"time rows={df_dtime.count()}")
log_check("fact_meter_reading_gte_0",    "fact_meter_readings",
          df_fact.filter("meter_reading < 0").count() == 0, "no negative readings")

# --- Referential Integrity ---
orphan_buildings = df_fact.join(df_dbldg, on="building_key", how="left_anti").count()
log_check("fact_building_key_resolves",  "fact_meter_readings",
          orphan_buildings == 0, f"orphan building_keys={orphan_buildings}")

orphan_sites = df_fact.join(df_dsite, on="site_key", how="left_anti").count()
log_check("fact_site_key_resolves",      "fact_meter_readings",
          orphan_sites == 0, f"orphan site_keys={orphan_sites}")

orphan_dates = df_fact.join(df_ddate, on="date_key", how="left_anti").count()
log_check("fact_date_key_resolves",      "fact_meter_readings",
          orphan_dates == 0, f"orphan date_keys={orphan_dates}")

orphan_meters = df_fact.join(df_dmeter, on="meter_type_key", how="left_anti").count()
log_check("fact_meter_type_key_resolves","fact_meter_readings",
          orphan_meters == 0, f"orphan meter_type_keys={orphan_meters}")

# --- Range Checks ---
log_check("air_temp_range",             "fact_meter_readings",
          df_fact.filter("air_temperature < -40 or air_temperature > 50")
                 .count() == 0, "air_temp between -40 and 50")

# --- Timeliness ---
month_count = df_fact.select(F.month("reading_date")).distinct().count()
log_check("data_spans_12_months",       "fact_meter_readings",
          month_count == 12, f"distinct months={month_count}")

site_month = df_fact.select("site_key", F.month("reading_date")).distinct() \
                    .groupBy("site_key").count().filter("count < 12").count()
log_check("all_sites_have_12_months",   "fact_meter_readings",
          site_month == 0, f"sites with <12 months={site_month}")

# COMMAND ----------

spark.createDataFrame(results).write.format("delta").mode("append").saveAsTable(LOG_TABLE)
print(f"\nGold quality log written → {LOG_TABLE}")
display(spark.table(LOG_TABLE).orderBy("checked_at", ascending=False).limit(25))