from pyspark import pipelines as dp
from pyspark.sql import functions as F, DataFrame


def flag_turbine_data(raw_df: DataFrame) -> DataFrame:
    """
    Shared Silver preparation for clean_turbine_data and extract_outliers:
    clean and standardize raw turbine data, flagging (not removing)
    power_output outliers.

    Pure function of a bronze-shaped DataFrame (columns: timestamp,
    turbine_id, wind_speed, wind_direction, power_output, plus optional
    metadata columns) so it can be unit tested with a local SparkSession,
    independent of how the data was ingested.

    Steps:
    - Parse timestamp / derive date.
    - Deduplicate on (timestamp, turbine_id) - required because Bronze can
      re-emit already-seen rows when a source file is reprocessed.
    - Impute missing wind_speed / wind_direction with the turbine's own
      median (power_output nulls are dropped upstream via
      @dp.expect_or_drop, since there's no sound way to impute the primary
      measurement being reported on).
    - Flag power_output outliers (is_outlier) using a per-turbine 3-sigma
      rule, keeping the turbine's mean/stddev alongside for context.
      is_outlier is null where power_output is null, so those rows are
      excluded by both the outlier and non-outlier filters downstream.
    - Stamp _ingestion_time with the time this row was processed by Silver,
      overwriting the value carried in from Bronze - each layer records its
      own audit timestamp rather than propagating the original one.
    """
    df = raw_df.withColumn("timestamp", F.col("timestamp").cast("timestamp"))
    df = df.withColumn("date", F.to_date("timestamp"))

    # Deduplicate based on timestamp + turbine_id
    df = df.dropDuplicates(["timestamp", "turbine_id"])

    # Flag missing values before imputation
    df = df.withColumn("missing_wind_speed", F.col("wind_speed").isNull())
    df = df.withColumn("missing_wind_direction", F.col("wind_direction").isNull())

    # Calculate turbine-specific medians for imputation
    medians = df.groupBy("turbine_id").agg(
        F.expr("percentile_approx(wind_speed, 0.5)").alias("med_ws"),
        F.expr("percentile_approx(wind_direction, 0.5)").alias("med_wd")
    )

    # Join and impute missing values
    df = df.join(medians, "turbine_id", "left")
    df = df.withColumn("wind_speed", F.coalesce(F.col("wind_speed"), F.col("med_ws")))
    df = df.withColumn("wind_direction", F.coalesce(F.col("wind_direction"), F.col("med_wd")))
    df = df.drop("med_ws", "med_wd")

    # Calculate turbine-specific statistics for outlier detection (3-sigma rule)
    stats = df.groupBy("turbine_id").agg(
        F.mean("power_output").alias("mean_p"),
        F.stddev("power_output").alias("std_p")
    )

    # Join and flag outliers. std_p is null when a turbine has fewer than
    # two readings (stddev is undefined for n<2) - coalesce to 0 so those
    # readings are kept rather than being dropped by a null comparison.
    df = df.join(stats, "turbine_id", "left")
    df = df.withColumn("std_p", F.coalesce(F.col("std_p"), F.lit(0.0)))
    df = df.withColumn("is_outlier",
        (F.col("power_output") > F.col("mean_p") + 3 * F.col("std_p")) |
        (F.col("power_output") < F.col("mean_p") - 3 * F.col("std_p"))
    )

    # Calculate data quality score
    df = df.withColumn("quality_score",
        F.when(~F.col("missing_wind_speed") & ~F.col("missing_wind_direction"), 1.0)
        .when(F.col("missing_wind_speed") | F.col("missing_wind_direction"), 0.8)
        .otherwise(0.6)
    )

    # Audit column: overwrite Bronze's _ingestion_time with Silver's own
    return df.withColumn("_ingestion_time", F.current_timestamp())


def clean_turbine_data(raw_df: DataFrame) -> DataFrame:
    """
    Silver layer transformation: the rows of flag_turbine_data that aren't
    power_output outliers. The outliers go to extract_outliers instead.
    """
    df = flag_turbine_data(raw_df)
    return df.filter(~F.col("is_outlier")).select(
        "timestamp", "date", "turbine_id", "wind_speed", "wind_direction",
        "power_output", "quality_score", "missing_wind_speed", "missing_wind_direction",
        "_source_file", "_ingestion_time"
    )


def extract_outliers(raw_df: DataFrame) -> DataFrame:
    """
    Silver layer transformation: the power_output outliers excluded from
    clean_turbine_data, with the per-turbine mean/stddev they were judged
    against.
    """
    df = flag_turbine_data(raw_df)
    return df.filter(F.col("is_outlier")).select(
        "timestamp", "date", "turbine_id", "wind_speed", "wind_direction",
        "power_output",
        F.col("mean_p").alias("turbine_mean_power"),
        F.col("std_p").alias("turbine_stddev_power"),
        "quality_score", "missing_wind_speed", "missing_wind_direction",
        "_source_file", "_ingestion_time"
    )


@dp.materialized_view(
    name="colibri_assessment.silver.turbine_cleaned",
    comment="Cleaned wind turbine data with deduplication, missing value handling, and outlier removal",
    table_properties={"quality": "silver"},
    cluster_by=["turbine_id", "date"]
)
@dp.expect_or_fail("valid_timestamp", "timestamp IS NOT NULL")
@dp.expect_or_drop("valid_power_output", "power_output IS NOT NULL")
def turbine_cleaned():
    """Silver layer: Databricks pipeline entry point, delegates to clean_turbine_data."""
    raw_df = spark.read.table("colibri_assessment.bronze.turbine_raw")
    return clean_turbine_data(raw_df)


# Defined here rather than in its own file because it shares
# flag_turbine_data with turbine_cleaned: importing this module from another
# pipeline source file would register turbine_cleaned a second time.
@dp.materialized_view(
    name="colibri_assessment.silver.turbine_power_outliers",
    comment="power_output readings excluded from turbine_cleaned as per-turbine 3-sigma outliers",
    table_properties={"quality": "silver"},
    cluster_by=["turbine_id", "date"]
)
def turbine_power_outliers():
    """Silver layer: Databricks pipeline entry point, delegates to extract_outliers."""
    raw_df = spark.read.table("colibri_assessment.bronze.turbine_raw")
    return extract_outliers(raw_df)
