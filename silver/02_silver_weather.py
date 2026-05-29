# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import Window

SOURCE_TABLE    = "gridwatch360.energy.bronze_weather_readings"
TARGET_TABLE    = "gridwatch360.energy.silver_weather"
CHECKPOINT_PATH = "/Volumes/gridwatch360/energy/checkpoints/silver_weather/"

# COMMAND ----------

df_bronze = (
    spark.readStream
        .format("delta")
        .table(SOURCE_TABLE)
)

# COMMAND ----------

NULLABLE_COLS = ["cloud_coverage", "precip_depth_1_hr", "sea_level_pressure", "wind_direction", "wind_speed"]
NON_NEGATIVE_COLS = ["wind_speed", "precip_depth_1_hr", "sea_level_pressure", "cloud_coverage"]

def transform_weather(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    df = (
        batch_df
        .withColumn("timestamp",        F.to_timestamp("timestamp"))
        .withColumn("site_id",          F.col("site_id").cast("int"))
        .withColumn("air_temperature",  F.col("air_temperature").cast("double"))
        .withColumn("wind_speed",       F.col("wind_speed").cast("double"))
        .withColumn("wind_direction",   F.col("wind_direction").cast("double"))
        .withColumn("cloud_coverage",   F.col("cloud_coverage").cast("double"))
        .withColumn("dew_temperature",  F.col("dew_temperature").cast("double"))
        .withColumn("precip_depth_1_hr",F.col("precip_depth_1_hr").cast("double"))
        .withColumn("sea_level_pressure",F.col("sea_level_pressure").cast("double"))

        # derived time cols
        .withColumn("weather_date",     F.to_date("timestamp"))
        .withColumn("weather_hour",     F.hour("timestamp"))

        # wind_direction fix: if wind_speed=0 and wind_direction=0 → null
        .withColumn(
            "wind_direction",
            F.when(
                (F.col("wind_speed") == 0) & (F.col("wind_direction") == 0),
                F.lit(None).cast("double")
            ).otherwise(F.col("wind_direction"))
        )
        .drop("_rescued_data")
    )

    for col_name in NON_NEGATIVE_COLS:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name).isNotNull(), F.abs(F.col(col_name)))
            .otherwise(F.col(col_name))
        )

    # --- forward-fill nulls per site ordered by timestamp ---
    # PySpark doesn't have native ffill; use last(ignorenulls) over window
    site_time_window = (
        Window.partitionBy("site_id")
              .orderBy("timestamp")
              .rowsBetween(Window.unboundedPreceding, 0)
    )

    for col_name in ["cloud_coverage", "precip_depth_1_hr", "sea_level_pressure"]:
        df = df.withColumn(
            col_name,
            F.last(col_name, ignorenulls=True).over(site_time_window)
        )

    # --- fill remaining nulls with site median ---
    site_medians = (
        df.groupBy("site_id")
          .agg(
              F.percentile_approx("cloud_coverage",    0.5).alias("med_cloud"),
              F.percentile_approx("precip_depth_1_hr", 0.5).alias("med_precip"),
              F.percentile_approx("sea_level_pressure",0.5).alias("med_pressure"),
          )
    )

    df = (
        df.join(site_medians, on="site_id", how="left")
          .withColumn("cloud_coverage",    F.coalesce("cloud_coverage",    "med_cloud"))
          .withColumn("precip_depth_1_hr", F.coalesce("precip_depth_1_hr", "med_precip"))
          .withColumn("sea_level_pressure",F.coalesce("sea_level_pressure","med_pressure"))
          .drop("med_cloud", "med_precip", "med_pressure")
    )

    # --- temp_category ---
    df = df.withColumn(
        "temp_category",
        F.when(F.col("air_temperature") <  0,  "Freezing")
         .when(F.col("air_temperature") < 10,  "Cold")
         .when(F.col("air_temperature") < 20,  "Mild")
         .when(F.col("air_temperature") < 30,  "Warm")
         .otherwise("Hot")
    )

    # --- write ---
    (
        df.write
          .format("delta")
          .mode("append")
          .option("mergeSchema", "true")
          .partitionBy("weather_date")
          .saveAsTable(TARGET_TABLE)
    )

    print(f"[Batch {batch_id}] Weather rows written: {df.count():,}")

# COMMAND ----------

(
    df_bronze.writeStream
        .trigger(availableNow=True)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .foreachBatch(transform_weather)
        .start()
        .awaitTermination()
)

# COMMAND ----------

df = spark.table(TARGET_TABLE)
print(f"Total rows     : {df.count():,}")
print(f"Null air_temp  : {df.filter('air_temperature is null').count()}")
print(f"Temp categories: ")
display(df.groupBy("temp_category").count().orderBy("temp_category"))