from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="colibri_assessment.gold.turbine_daily_stats",
    comment="Daily summary statistics per turbine - aggregated metrics for monitoring and reporting",
    table_properties={"quality": "gold"},
    cluster_by=["turbine_id", "date"]
)
def turbine_daily_stats():
    """
    Gold layer: Daily summary statistics per turbine.
    
    Aggregates:
    - Min, max, avg, stddev of power output
    - Record count per day
    - Data quality score (% of valid readings)
    """
    # Read from Silver layer using batch read (materialized view)
    df = spark.read.table("colibri_assessment.silver.turbine_cleaned")
    
    # Aggregate by turbine_id and date
    stats_df = df.groupBy("turbine_id", "date").agg(
        F.min("power_output").alias("min_power_mw"),
        F.max("power_output").alias("max_power_mw"),
        F.avg("power_output").alias("avg_power_mw"),
        F.stddev("power_output").alias("std_dev_power_mw"),
        F.count("*").alias("record_count"),
        # Calculate data quality score as average of quality_score column
        F.avg("quality_score").alias("avg_quality_score"),
        # Calculate percentage of high-quality records (quality_score = 1.0)
        (F.sum(F.when(F.col("quality_score") == 1.0, 1).otherwise(0)) / F.count("*") * 100).alias("pct_high_quality_readings")
    )
    
    # Add computed metrics
    stats_df = stats_df.withColumn("power_range_mw", 
        F.col("max_power_mw") - F.col("min_power_mw"))
    
    # Sort by date and turbine_id for easier consumption
    return stats_df.orderBy("date", "turbine_id")