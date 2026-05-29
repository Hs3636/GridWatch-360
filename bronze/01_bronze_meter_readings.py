# Databricks notebook source
CATALOG          = "gridwatch360"
SCHEMA           = "energy"
SOURCE_PATH      = "/Volumes/gridwatch360/energy/raw_uploads/meter_readings/"
TARGET_TABLE     = "gridwatch360.energy.bronze_meter_readings"
CHECKPOINT_PATH  = "/Volumes/gridwatch360/energy/checkpoints/bronze_meter/"
SCHEMA_PATH      = "/Volumes/gridwatch360/energy/schema_store/bronze_meter/"

# COMMAND ----------

df_raw = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)   # Volume path — already correct
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.useNotifications", "false")    
        .option("header", "true")
        .load(SOURCE_PATH)
)

# COMMAND ----------

from pyspark.sql import functions as F
import uuid

def add_bronze_metadata(df, source_path):
    return (
        df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("ingestion_date",      F.current_date())
        .withColumn("source_file_name",    F.col("_metadata.file_path"))
        .withColumn("batch_id",            F.lit(str(uuid.uuid4())))
    )

df_bronze = add_bronze_metadata(df_raw, SOURCE_PATH)

# COMMAND ----------

(
    df_bronze.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)          # processes all pending files, then stops — perfect for CE serverless
        .toTable(TARGET_TABLE)
)

# COMMAND ----------

df_check = spark.table(TARGET_TABLE)
print(f"Row count : {df_check.count():,}")
print(f"Partitions: {df_check.select('ingestion_date').distinct().count()}")
display(df_check.limit(5))