from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="colibri_assessment.silver.turbine_cleaned",
    comment="Cleaned wind turbine data with deduplication, missing value handling, and outlier removal",
    table_properties={"quality": "silver"},
    cluster_by=["turbine_id", "date"]
)
@dp.expect_or_fail("valid_timestamp", "timestamp IS NOT NULL")
@dp.expect_or_drop("valid_power_output", "power_output IS NOT NULL")
def turbine_cleaned():
    """
    Silver layer: Clean and transform raw turbine data.
    Uses batch processing (Materialized View) for complex transformations.
    """
    # Read bronze data
    df = spark.read.table("colibri_assessment.bronze.turbine_raw")
    
    # Parse timestamp and extract date
    df = df.withColumn("timestamp", F.col("timestamp").cast("timestamp"))
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
    
    # Join and flag outliers
    df = df.join(stats, "turbine_id", "left")
    df = df.withColumn("is_outlier",
        (F.col("power_output") > F.col("mean_p") + 3 * F.col("std_p")) |
        (F.col("power_output") < F.col("mean_p") - 3 * F.col("std_p"))
    )
    
    # Remove outliers
    df = df.filter(~F.col("is_outlier"))
    
    # Calculate data quality score
    df = df.withColumn("quality_score",
        F.when(~F.col("missing_wind_speed") & ~F.col("missing_wind_direction"), 1.0)
        .when(F.col("missing_wind_speed") | F.col("missing_wind_direction"), 0.8)
        .otherwise(0.6)
    )
    
    # Select final columns
    return df.select(
        "timestamp", "date", "turbine_id", "wind_speed", "wind_direction",
        "power_output", "quality_score", "missing_wind_speed", "missing_wind_direction",
        "_source_file", "_ingestion_time"
    )