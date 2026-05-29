# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import uuid

CATALOG         = "gridwatch360"
SCHEMA          = "energy"
SOURCE_PATH     = "/Volumes/gridwatch360/energy/raw_uploads/building_metadata/"
TARGET_TABLE    = "gridwatch360.energy.bronze_building_metadata"
CHECKPOINT_PATH = "/Volumes/gridwatch360/energy/checkpoints/bronze_building/"
SCHEMA_PATH     = "/Volumes/gridwatch360/energy/schema_store/bronze_building/"
BATCH_ID        = str(uuid.uuid4())

# COMMAND ----------

# foreachBatch pattern: AutoLoader detects new files, we apply SCD2 in the batch function
df_raw = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .load(SOURCE_PATH)
)

# COMMAND ----------

# DBTITLE 1,Cell 3
def apply_scd2_building(batch_df, batch_id_exec):
    if batch_df.isEmpty():
        return

    # Add metadata to incoming batch
    incoming = (
        batch_df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("ingestion_date",      F.current_date())
        .withColumn("source_file_name",    F.col("_metadata.file_path"))
        .withColumn("batch_id",            F.lit(BATCH_ID))
        .withColumn("effective_from",      F.current_date())
        .withColumn("effective_to",        F.lit("9999-12-31").cast("date"))
        .withColumn("is_current",          F.lit(True))
    )

    # Check if target table exists
    if not spark.catalog.tableExists(TARGET_TABLE):
        incoming.drop("_rescued_data").write.format("delta").mode("append").saveAsTable(TARGET_TABLE)
        print(f"[SCD2] Initial load: {incoming.count()} rows written.")
        return

    target = DeltaTable.forName(spark, TARGET_TABLE)

    # Step 1: Expire changed records in target
    # A "change" = same building_id but different primary_use (the SCD2 tracked column)
    target.alias("tgt").merge(
        incoming.alias("src"),
        "tgt.building_id = src.building_id AND tgt.is_current = true"
    ).whenMatchedUpdate(
        condition="tgt.primary_use != src.primary_use",
        set={
            "is_current":   "false",
            "effective_to": "current_date()"
        }
    ).execute()

    # Step 2: Insert new/changed records
    # Only insert rows that are either new buildings or changed primary_use
    existing = spark.table(TARGET_TABLE).filter(F.col("is_current") == True)

    new_or_changed = (
        incoming.alias("src")
        .join(existing.alias("tgt"), on="building_id", how="left")
        .filter(
            F.col("tgt.building_id").isNull() |                          # new building
            (F.col("src.primary_use") != F.col("tgt.primary_use"))       # use type changed
        )
        .select("src.*")
    )

    if new_or_changed.count() > 0:
        new_or_changed.drop("_rescued_data").write.format("delta").mode("append").saveAsTable(TARGET_TABLE)
        print(f"[SCD2] Inserted {new_or_changed.count()} new/changed rows.")
    else:
        print("[SCD2] No changes detected.")

# COMMAND ----------

(
    df_raw.writeStream
        .format("delta")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .foreachBatch(apply_scd2_building)
        .start()
        .awaitTermination()
)

# COMMAND ----------

df_check = spark.table(TARGET_TABLE)
print(f"Total rows (all versions): {df_check.count()}")
print(f"Current rows             : {df_check.filter('is_current = true').count()}")
display(df_check.orderBy("building_id").limit(10))