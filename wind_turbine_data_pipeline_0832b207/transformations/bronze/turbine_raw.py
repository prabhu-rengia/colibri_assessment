from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.temporary_view()
def turbine_raw_cdc():
    """
    Streaming source for the CDC merge below: Auto Loader ingest of the raw CSVs.

    Each turbine's CSV (data_group_1.csv, data_group_2.csv, ...) is updated
    in place daily - the same file is appended to, rather than a new file
    landing each day. Auto Loader's default file-discovery mode reads a
    given file once; `cloudFiles.allowOverwrites` makes it watch file
    modification times and reprocess a file end-to-end when it changes.
    CSV has no byte-offset resume point, so that reprocessing re-emits every
    row already seen, not just the new ones. `create_auto_cdc_flow` (below)
    merges this stream into the target table by (turbine_id, timestamp)
    instead of appending it, so re-emitted rows just no-op there rather than
    piling up as duplicates on every file change.

    Note this fixes *storage*, not *compute*: this view is a temporary view
    with no state of its own, so every micro-batch triggered by a file
    change still re-reads and re-emits that file's full current contents
    (there's no cheaper option for CSV). The merge only discards the
    resulting duplicates on write. E.g. a file that grows 100 -> 210 -> 230
    rows over three days costs ~540 rows of read/merge work across those
    three runs, even though the target table only ever holds the current
    230. Acceptable at this data volume; would need revisiting for large
    per-file sizes.
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


dp.create_streaming_table(
    name="colibri_assessment.bronze.turbine_raw",
    comment="Raw wind turbine sensor data, merged from CSV files via Auto Loader + CDC upsert",
    table_properties={"quality": "bronze"},
    expect_all={
        "valid_timestamp_format": "timestamp IS NOT NULL",
        "valid_turbine_id": "turbine_id IS NOT NULL",
        "non_negative_power": "power_output IS NULL OR power_output >= 0",
    },
)

dp.create_auto_cdc_flow(
    target="colibri_assessment.bronze.turbine_raw",
    source="turbine_raw_cdc",
    keys=["turbine_id", "timestamp"],
    sequence_by="_ingestion_time",
    stored_as_scd_type="1",
)
