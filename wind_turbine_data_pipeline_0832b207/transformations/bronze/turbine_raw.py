from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Rows missing either merge key can't be upserted into turbine_raw (and a
# null timestamp would fail Silver's expect_or_fail), so they're routed to a
# quarantine table instead. The two predicates are exact complements: IS NULL
# never evaluates to null, so every row lands in exactly one table.
_INVALID_KEYS = "turbine_id IS NULL OR timestamp IS NULL"
_VALID_KEYS = f"NOT ({_INVALID_KEYS})"


@dp.temporary_view()
def turbine_raw_ingest():
    """
    Auto Loader ingest of the raw CSVs, shared by the two views below that
    split it into valid rows (-> turbine_raw) and quarantined rows
    (-> turbine_raw_quarantine).

    Each turbine's CSV (data_group_1.csv, data_group_2.csv, ...) is updated
    in place daily - the same file is appended to, rather than a new file
    landing each day. Auto Loader's default file-discovery mode reads a
    given file once; `cloudFiles.allowOverwrites` makes it watch file
    modification times and reprocess a file end-to-end when it changes.
    CSV has no byte-offset resume point, so that reprocessing re-emits every
    row already seen, not just the new ones. Both targets below are
    therefore CDC merges rather than appends, so re-emitted rows just no-op
    there rather than piling up as duplicates on every file change.

    Note this fixes *storage*, not *compute*: this view is a temporary view
    with no state of its own, so every micro-batch triggered by a file
    change still re-reads and re-emits that file's full current contents
    (there's no cheaper option for CSV). The merge only discards the
    resulting duplicates on write. E.g. a file that grows 100 -> 210 -> 230
    rows over three days costs ~540 rows of read/merge work across those
    three runs, even though the target table only ever holds the current
    230. Acceptable at this data volume; would need revisiting for large
    per-file sizes. Each of the two downstream flows also runs its own
    Auto Loader stream over this view, so the source is read twice per run.

    PERMISSIVE mode keeps unparseable rows rather than dropping them: fields
    that don't fit the inferred type come through as null, with the original
    value preserved in Auto Loader's `_rescued_data` column.
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


@dp.temporary_view()
def turbine_raw_cdc():
    """Streaming source for the turbine_raw CDC merge: rows with both merge keys present."""
    return spark.readStream.table("turbine_raw_ingest").filter(_VALID_KEYS)


@dp.temporary_view()
def turbine_raw_quarantine_cdc():
    """
    Streaming source for the quarantine CDC merge: rows missing turbine_id
    and/or timestamp.

    The natural key is exactly what's null here, so rows are keyed instead
    on (_source_file, _row_hash), a hash of the row's full contents
    (excluding _ingestion_time, which differs on every re-emit). That lets
    the merge collapse the copies that allowOverwrites re-emits each time
    the file changes, the same way turbine_raw's merge does.
    """
    df = spark.readStream.table("turbine_raw_ingest").filter(_INVALID_KEYS)
    content_cols = [c for c in df.columns if c != "_ingestion_time"]
    return (
        df
        .withColumn(
            "_quarantine_reason",
            F.when(F.col("turbine_id").isNull() & F.col("timestamp").isNull(),
                   "missing turbine_id and timestamp")
            .when(F.col("turbine_id").isNull(), "missing turbine_id")
            .otherwise("missing timestamp"),
        )
        .withColumn("_row_hash", F.sha2(F.to_json(F.struct(*content_cols)), 256))
    )


dp.create_streaming_table(
    name="colibri_assessment.bronze.turbine_raw",
    comment="Raw wind turbine sensor data, merged from CSV files via Auto Loader + CDC upsert",
    table_properties={"quality": "bronze"},
    expect_all={
        "non_negative_wind_speed": "wind_speed IS NULL OR wind_speed >= 0",
        "valid_wind_direction": "wind_direction IS NULL OR (wind_direction >= 0 AND wind_direction < 360)",
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

dp.create_streaming_table(
    name="colibri_assessment.bronze.turbine_raw_quarantine",
    comment="Raw rows rejected from turbine_raw for a missing turbine_id and/or timestamp",
    table_properties={"quality": "bronze"},
)

dp.create_auto_cdc_flow(
    target="colibri_assessment.bronze.turbine_raw_quarantine",
    source="turbine_raw_quarantine_cdc",
    keys=["_source_file", "_row_hash"],
    sequence_by="_ingestion_time",
    stored_as_scd_type="1",
)
