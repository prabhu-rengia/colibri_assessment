from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="colibri_assessment.bronze.turbine_raw",
    comment="Raw wind turbine sensor data ingested from CSV files using Auto Loader",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"}
)
@dp.expect("valid_timestamp_format", "timestamp IS NOT NULL")
@dp.expect("valid_turbine_id", "turbine_id IS NOT NULL")
@dp.expect("non_negative_power", "power_output IS NULL OR power_output >= 0")
def turbine_raw():
    """
    Bronze layer: Ingest raw CSV files from Volume using Auto Loader.

    Each turbine's CSV (data_group_1.csv, data_group_2.csv, ...) is updated
    in place daily with the latest 24h of readings appended to the same
    file, rather than a new file landing each day. Auto Loader's default
    file-discovery mode only reads a given file once, so a plain streaming
    load would silently miss every day's new rows after the first ingest.
    `cloudFiles.allowOverwrites` tells Auto Loader to watch file
    modification times and reprocess a file end-to-end when it changes.
    That means already-seen rows get re-emitted on every update, which is
    safe here because the Silver layer deduplicates on
    (timestamp, turbine_id) before anything downstream consumes the data.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.allowOverwrites", "true")
        .option("mode", "PERMISSIVE")
        .load("/Volumes/colibri_assessment/default/colibri_input/")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingestion_time", F.current_timestamp())
    )