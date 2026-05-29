# Databricks notebook source
from pyspark.sql import functions as F

READINGS_TABLE  = "gridwatch360.energy.silver_readings"
WEATHER_TABLE   = "gridwatch360.energy.silver_weather"
BUILDING_TABLE  = "gridwatch360.energy.bronze_building_metadata"
TARGET_TABLE    = "gridwatch360.energy.silver_enriched_readings"
CHECKPOINT_PATH = "/Volumes/gridwatch360/energy/checkpoints/silver_enriched/"

# COMMAND ----------

# Building: only current SCD2 records
df_building = (
    spark.table(BUILDING_TABLE)
         .filter(F.col("is_current") == True)
         .select("building_id", "site_id", "primary_use", "square_feet",
                 "year_built", "floor_count", "effective_from")
)

# Weather: join key = site_id + date + hour
df_weather = (
    spark.table(WEATHER_TABLE)
         .select("site_id", "weather_date", "weather_hour",
                 "air_temperature", "wind_speed", "temp_category",
                 "sea_level_pressure", "dew_temperature")
)

# COMMAND ----------

# DBTITLE 1,Cell 3
def enrich_readings(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # --- join building metadata ---
    with_building = (
        batch_df
        .join(df_building, on="building_id", how="left")
        .withColumn(
            "is_building_unknown",
            F.col("site_id").isNull()               # no match in metadata
        )
    )

    # --- join weather on site_id + date + hour ---
    with_weather = (
        with_building.alias("readings")
        .join(
            df_weather.alias("weather"),
            on=[
                F.col("readings.site_id") == F.col("weather.site_id"),
                F.col("readings.reading_date") == F.col("weather.weather_date"),
                F.col("readings.reading_hour") == F.col("weather.weather_hour"),
            ],
            how="left"
        )
        .select(
            "readings.*",
            F.col("weather.air_temperature").alias("air_temperature"),
            F.col("weather.wind_speed").alias("wind_speed"),
            F.col("weather.temp_category").alias("temp_category"),
            F.col("weather.sea_level_pressure").alias("sea_level_pressure"),
            F.col("weather.dew_temperature").alias("dew_temperature")
        )
    )

    # --- energy per sqft ---
    enriched = (
        with_weather
        .withColumn(
            "energy_per_sqft",
            F.when(
                F.col("square_feet").isNotNull() & (F.col("square_feet") > 0),
                F.round(F.col("meter_reading") / F.col("square_feet"), 6)
            ).otherwise(F.lit(None).cast("double"))
        )
        # building age
        .withColumn(
            "building_age",
            F.when(
                F.col("year_built").isNotNull(),
                F.lit(2016) - F.col("year_built")
            ).otherwise(F.lit(None).cast("int"))
        )
        # floor_count null fill
        .withColumn("floor_count", F.coalesce(F.col("floor_count"), F.lit(0)))

        # flags
        .withColumn("is_year_unknown",  F.col("year_built").isNull())
        .withColumn("is_floor_unknown", F.col("floor_count") == 0)
    )

    (
        enriched.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .partitionBy("reading_date")
                .saveAsTable(TARGET_TABLE)
    )

    print(f"[Batch {batch_id}] Enriched rows: {enriched.count():,}")

# COMMAND ----------

df_readings_stream = (
    spark.readStream
         .format("delta")
         .table(READINGS_TABLE)
)

(
    df_readings_stream.writeStream
        .trigger(availableNow=True)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .foreachBatch(enrich_readings)
        .start()
        .awaitTermination()
)

# COMMAND ----------

df = spark.table(TARGET_TABLE)
print(f"Total rows            : {df.count():,}")
print(f"Unknown buildings     : {df.filter('is_building_unknown = true').count():,}")
print(f"Null energy_per_sqft  : {df.filter('energy_per_sqft is null').count():,}")
print(f"Spikes                : {df.filter('is_spike = true').count():,}")
display(df.limit(5))