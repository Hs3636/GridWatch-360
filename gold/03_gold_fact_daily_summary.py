# Databricks notebook source
from pyspark.sql import functions as F
import uuid

SOURCE_TABLE    = "gridwatch360.energy.fact_meter_readings"
TARGET_TABLE    = "gridwatch360.energy.fact_daily_building_summary"
CHECKPOINT_PATH = "/Volumes/gridwatch360/energy/checkpoints/gold_fact_daily/"
BATCH_ID        = str(uuid.uuid4())

# COMMAND ----------

def build_fact_daily(batch_df, batch_id_exec):
    if batch_df.isEmpty():
        return

    agg = (
        batch_df
        .groupBy("date_key", "building_key", "site_key", "reading_date")
        .agg(
            # meter-type pivoted sums
            F.sum(F.when(F.col("meter_type_key") == 0, F.col("meter_reading")).otherwise(0))
             .alias("total_electricity_kwh"),
            F.sum(F.when(F.col("meter_type_key") == 1, F.col("meter_reading")).otherwise(0))
             .alias("total_chilled_water"),
            F.sum(F.when(F.col("meter_type_key") == 2, F.col("meter_reading")).otherwise(0))
             .alias("total_steam"),
            F.sum(F.when(F.col("meter_type_key") == 3, F.col("meter_reading")).otherwise(0))
             .alias("total_hot_water"),

            # total energy
            F.sum("meter_reading").alias("total_energy"),

            # weather
            F.avg("air_temperature").alias("avg_temperature"),

            # completeness
            F.countDistinct("time_of_day_key")
             .alias("hours_with_data"),
            F.sum(F.when(F.col("is_zero_reading") == True, 1).otherwise(0))
             .alias("zero_reading_hours"),
            F.sum(F.when(F.col("is_spike") == True, 1).otherwise(0))
             .alias("spike_count"),
        )

        # derived columns
        .withColumn("total_energy",
            F.col("total_electricity_kwh") + F.col("total_chilled_water") +
            F.col("total_steam") + F.col("total_hot_water")
        )
        .withColumn(
            "data_completeness_pct",
            F.round(F.col("hours_with_data") / F.lit(24) * 100, 2)
        )

        # composite PK
        .withColumn(
            "building_date_key",
            F.sha2(
                F.concat_ws("||",
                    F.col("building_key"),
                    F.col("date_key").cast("string")
                ), 256
            )
        )

        .select(
            "building_date_key", "date_key", "building_key", "site_key",
            "total_electricity_kwh", "total_chilled_water", "total_steam", "total_hot_water",
            "total_energy", "avg_temperature", "hours_with_data",
            "zero_reading_hours", "spike_count", "data_completeness_pct",
            "reading_date"
        )
    )

    # energy_per_sqft_day requires square_feet from dim_building
    dim_building = (
        spark.table("gridwatch360.energy.dim_building")
             .filter("is_current = true")
             .select("building_key", "square_feet")
    )

    final = (
        agg.join(F.broadcast(dim_building), on="building_key", how="left")
           .withColumn(
               "energy_per_sqft_day",
               F.when(
                   F.col("square_feet").isNotNull() & (F.col("square_feet") > 0),
                   F.round(F.col("total_energy") / F.col("square_feet"), 6)
               ).otherwise(F.lit(None).cast("double"))
           )
           .drop("square_feet")
    )

    (
        final.write
             .format("delta")
             .mode("append")
             .option("mergeSchema", "true")
             .partitionBy("reading_date")
             .saveAsTable(TARGET_TABLE)
    )

    print(f"[Batch {batch_id_exec}] Daily summary rows: {final.count():,}")

# COMMAND ----------

df_stream = (
    spark.readStream
         .format("delta")
         .table(SOURCE_TABLE)
)

(
    df_stream.writeStream
        .trigger(availableNow=True)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .foreachBatch(build_fact_daily)
        .start()
        .awaitTermination()
)

# COMMAND ----------

df = spark.table(TARGET_TABLE)
print(f"Total rows                : {df.count():,}")
print(f"Null building_date_key    : {df.filter('building_date_key is null').count()}")
print(f"Avg data_completeness_pct : {df.agg(F.avg('data_completeness_pct')).collect()[0][0]:.2f}%")
display(df.orderBy("date_key").limit(5))