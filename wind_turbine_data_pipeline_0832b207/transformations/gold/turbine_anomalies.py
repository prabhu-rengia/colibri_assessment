from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(
    name="colibri_assessment.gold.turbine_anomalies",
    comment="Anomaly detection for wind turbine power output using rolling 7-day baseline",
    table_properties={"quality": "gold"},
    cluster_by=["turbine_id", "date"]
)
def turbine_anomalies():
    """
    Gold layer: Anomaly detection for turbine power output.
    
    Logic:
    - Calculate rolling 7-day baseline (mean and stddev) per turbine
    - Flag days where avg power output is > 2 std deviations from baseline
    - Provide context: expected range, actual power, anomaly score
    """
    # Read daily stats from Gold layer
    df = spark.read.table("colibri_assessment.gold.turbine_daily_stats")
    
    # Define rolling 7-day window (excluding current day to establish baseline)
    window_spec = (
        Window.partitionBy("turbine_id")
        .orderBy("date")
        .rowsBetween(-7, -1)  # Previous 7 days only
    )
    
    # Calculate rolling baseline statistics
    df = df.withColumn("baseline_mean_power", F.avg("avg_power_mw").over(window_spec))
    df = df.withColumn("baseline_stddev_power", F.stddev("avg_power_mw").over(window_spec))
    
    # Calculate expected range (mean ± 2 std deviations)
    df = df.withColumn("expected_range_lower",
        F.col("baseline_mean_power") - 2 * F.col("baseline_stddev_power"))
    df = df.withColumn("expected_range_upper",
        F.col("baseline_mean_power") + 2 * F.col("baseline_stddev_power"))
    
    # Calculate anomaly score (how many std deviations from baseline)
    df = df.withColumn("anomaly_score",
        F.abs(F.col("avg_power_mw") - F.col("baseline_mean_power")) / 
        F.coalesce(F.col("baseline_stddev_power"), F.lit(1.0))  # Avoid division by zero
    )
    
    # Flag anomalous days (> 2 std deviations)
    df = df.withColumn("is_anomalous",
        (F.col("avg_power_mw") < F.col("expected_range_lower")) |
        (F.col("avg_power_mw") > F.col("expected_range_upper"))
    )
    
    # Add anomaly reason
    df = df.withColumn("anomaly_reason",
        F.when(F.col("avg_power_mw") > F.col("expected_range_upper"), "Power output unusually HIGH")
        .when(F.col("avg_power_mw") < F.col("expected_range_lower"), "Power output unusually LOW")
        .otherwise("Normal operation")
    )
    
    # Select relevant columns
    result_df = df.select(
        "date",
        "turbine_id",
        "avg_power_mw",
        "baseline_mean_power",
        "baseline_stddev_power",
        "expected_range_lower",
        "expected_range_upper",
        "anomaly_score",
        "is_anomalous",
        "anomaly_reason",
        "record_count",
        "avg_quality_score"
    )
    
    # Filter to only anomalous days (optional - comment out to keep all days)
    # result_df = result_df.filter(F.col("is_anomalous"))
    
    return result_df.orderBy("date", "turbine_id")