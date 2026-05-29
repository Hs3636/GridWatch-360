# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import uuid

SOURCE_TABLE    = "gridwatch360.energy.silver_enriched_readings"
TARGET_TABLE    = "gridwatch360.energy.fact_meter_readings"
CHECKPOINT_PATH = "/Volumes/gridwatch360/energy/checkpoints/gold_fact_meter/"
BATCH_ID        = str(uuid.uuid4())

# COMMAND ----------

dim_date     = spark.table("gridwatch360.energy.dim_date") \
                    .select("date_key", "date")
dim_building = spark.table("gridwatch360.energy.dim_building") \
                    .filter("is_current = true") \
                    .select("building_key", "building_id")
dim_site     = spark.table("gridwatch360.energy.dim_site") \
                    .select("site_key", "site_id")
dim_meter    = spark.table("gridwatch360.energy.dim_meter_type") \
                    .select("meter_type_key", "meter_label")
dim_time     = spark.table("gridwatch360.energy.dim_time_of_day") \
                    .select("time_of_day_key", "hour")

# COMMAND ----------

def build_fact_meter(batch_df, batch_id_exec):
    if batch_df.isEmpty():
        return

    fact = (
        batch_df

        # --- FK: DIM_DATE ---
        .join(F.broadcast(dim_date),
              batch_df.reading_date == dim_date.date, how="left")

        # --- FK: DIM_BUILDING ---
        .join(F.broadcast(dim_building),
              on="building_id", how="left")

        # --- FK: DIM_SITE ---
        .join(F.broadcast(dim_site),
              on="site_id", how="left")

        # --- FK: DIM_METER_TYPE ---
        .join(F.broadcast(dim_meter),
              on="meter_label", how="left")

        # --- FK: DIM_TIME_OF_DAY ---
        .join(F.broadcast(dim_time),
              batch_df.reading_hour == dim_time.hour, how="left")

        # --- surrogate PK: hash of grain ---
        .withColumn(
            "reading_key",
            F.sha2(
                F.concat_ws("||",
                    F.col("building_id").cast("string"),
                    F.col("meter").cast("string"),
                    F.col("timestamp").cast("string")
                ), 256
            )
        )

        # --- select final columns ---
        .select(
            "reading_key",
            F.col("date_key"),
            F.col("building_key"),
            F.col("site_key"),
            F.col("meter_type_key"),
            F.col("time_of_day_key"),
            F.col("meter_reading"),
            F.col("energy_per_sqft"),
            F.col("is_zero_reading"),
            F.col("is_spike"),
            F.col("is_building_unknown"),
            F.col("air_temperature"),
            F.col("wind_speed"),
            F.col("temp_category"),
            F.col("source_file_name"),
            F.lit(BATCH_ID).alias("batch_id"),
            F.col("reading_date"),
        )
    )

    (
        fact.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("reading_date")
            .saveAsTable(TARGET_TABLE)
    )

    print(f"[Batch {batch_id_exec}] Fact rows written: {fact.count():,}")

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
        .foreachBatch(build_fact_meter)
        .start()
        .awaitTermination()
)

# COMMAND ----------

df = spark.table(TARGET_TABLE)
print(f"Total rows         : {df.count():,}")
print(f"Null building_key  : {df.filter('building_key is null').count():,}")
print(f"Null date_key      : {df.filter('date_key is null').count():,}")
print(f"Zero readings      : {df.filter('is_zero_reading = true').count():,}")
print(f"Spikes             : {df.filter('is_spike = true').count():,}")
display(df.limit(5))