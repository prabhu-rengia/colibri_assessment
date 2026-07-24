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
    Files are appended daily - deduplication happens in Silver layer.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("mode", "PERMISSIVE")
        .load("/Volumes/colibri_assessment/default/colibri_input/")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingestion_time", F.current_timestamp())
    )