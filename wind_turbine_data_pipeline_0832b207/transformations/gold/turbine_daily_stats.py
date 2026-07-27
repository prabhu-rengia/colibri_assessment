from pyspark import pipelines as dp
from pyspark.sql import functions as F, DataFrame


def compute_daily_stats(cleaned_df: DataFrame) -> DataFrame:
    """
    Gold layer transformation: daily summary statistics per turbine.

    Pure function of a silver-shaped DataFrame so it can be unit tested
    directly. Aggregates, per turbine per day:
    - min / max / avg / stddev of power output
    - record count
    - data quality score (share of readings that needed no imputation)
    """
    stats_df = cleaned_df.groupBy("turbine_id", "date").agg(
        F.min("power_output").alias("min_power_mw"),
        F.max("power_output").alias("max_power_mw"),
        F.avg("power_output").alias("avg_power_mw"),
        F.stddev("power_output").alias("std_dev_power_mw"),
        F.count("*").alias("record_count"),
        F.avg("quality_score").alias("avg_quality_score"),
        (F.sum(F.when(F.col("quality_score") == 1.0, 1).otherwise(0)) / F.count("*") * 100).alias("pct_high_quality_readings")
    )

    stats_df = stats_df.withColumn("power_range_mw",
        F.col("max_power_mw") - F.col("min_power_mw"))

    return stats_df.orderBy("date", "turbine_id")


@dp.materialized_view(
    name="colibri_assessment.gold.turbine_daily_stats",
    comment="Daily summary statistics per turbine - aggregated metrics for monitoring and reporting",
    table_properties={"quality": "gold"},
    cluster_by=["turbine_id", "date"]
)
def turbine_daily_stats():
    """Gold layer: Databricks pipeline entry point, delegates to compute_daily_stats."""
    df = spark.read.table("colibri_assessment.silver.turbine_cleaned")
    return compute_daily_stats(df)
