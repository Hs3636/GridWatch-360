# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable

SOURCE_TABLE    = "gridwatch360.energy.bronze_meter_readings"
TARGET_TABLE    = "gridwatch360.energy.silver_readings"
CHECKPOINT_PATH = "/Volumes/gridwatch360/energy/checkpoints/silver_readings/"

# COMMAND ----------

df_bronze = (
    spark.readStream
        .format("delta")
        .option("readChangeFeed", "false")
        .table(SOURCE_TABLE)
)

# COMMAND ----------

METER_LABEL_MAP = {0: "Electricity", 1: "Chilled Water", 2: "Steam", 3: "Hot Water"}
mapping_expr    = F.create_map([F.lit(x) for pair in METER_LABEL_MAP.items() for x in pair])

def transform_readings(df):
    return (
        df
        # --- type casting ---
        .withColumn("timestamp",        F.to_timestamp("timestamp"))
        .withColumn("meter_reading", F.round(F.col("meter_reading").cast("double"), 2))
        .withColumn("building_id",      F.col("building_id").cast("int"))
        .withColumn("meter",            F.col("meter").cast("int"))

        # --- derived time columns ---
        .withColumn("reading_date",         F.to_date("timestamp"))
        .withColumn("reading_hour",         F.hour("timestamp"))
        .withColumn("reading_day_of_week",  F.dayofweek("timestamp"))       # 1=Sun, 7=Sat
        .withColumn("is_weekend",           F.dayofweek("timestamp").isin(1, 7))
        .withColumn("reading_month",        F.month("timestamp"))

        # --- meter label ---
        .withColumn("meter_label", mapping_expr[F.col("meter")])

        # --- anomaly: zero reading ---
        .withColumn("is_zero_reading", F.col("meter_reading") == 0.0)

        # --- drop raw metadata cols not needed downstream ---
        .drop("_rescued_data")
    )

# COMMAND ----------

# DBTITLE 1,Cell 4
def apply_spike_detection(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # --- apply core transforms first ---
    transformed = transform_readings(batch_df)

    # --- rolling 7-day avg per building+meter using batch window ---
    window_spec = (
        Window.partitionBy("building_id", "meter")
              .orderBy(F.col("timestamp").cast("long"))
              .rangeBetween(-7 * 24 * 3600, 0)        # 7 days in seconds
    )

    with_rolling = (
        transformed
        .withColumn("rolling_avg_7d", F.avg("meter_reading").over(window_spec))
        .withColumn(
            "is_spike",
            (F.col("meter_reading") > 0) &
            (F.col("rolling_avg_7d") > 0) &
            (F.col("meter_reading") > 5 * F.col("rolling_avg_7d"))
        )
    )

    # --- write to silver ---
    (
        with_rolling
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("reading_date")
        .saveAsTable(TARGET_TABLE)
    )

    print(f"[Batch {batch_id}] Rows written: {with_rolling.count():,}")

# COMMAND ----------

(
    df_bronze.writeStream
        .trigger(availableNow=True)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .foreachBatch(apply_spike_detection)
        .start()
        .awaitTermination()
)

# COMMAND ----------

df = spark.table(TARGET_TABLE)
print(f"Total rows     : {df.count():,}")
print(f"Zero readings  : {df.filter('is_zero_reading = true').count():,}")
print(f"Spikes flagged : {df.filter('is_spike = true').count():,}")
display(df.limit(5))