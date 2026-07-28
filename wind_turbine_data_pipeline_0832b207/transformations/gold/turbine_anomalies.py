from pyspark import pipelines as dp
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.window import Window


def detect_anomalies(daily_stats_df: DataFrame) -> DataFrame:
    """
    Gold layer transformation: anomaly detection for turbine power output.

    Per the assignment spec, a turbine is anomalous when its output is
    outside 2 standard deviations from the mean "over the same time
    period" - i.e. compared against its peers on that same day, not
    against its own historical baseline. All turbines on the farm see
    broadly the same wind conditions, so for each date we compute the
    fleet's mean and standard deviation of avg_power_mw across all
    turbines that reported that day, and flag any turbine whose daily
    average falls outside mean +/- 2*stddev. This also means a turbine can
    be flagged from day one, with no rolling history required.

    Pure function of a gold daily-stats-shaped DataFrame so it can be unit
    tested directly.

    Stamps _ingestion_time with the time this anomaly check was computed -
    its own audit timestamp, independent of turbine_daily_stats's.
    """
    same_day = Window.partitionBy("date")

    df = daily_stats_df.withColumn("fleet_mean_power", F.avg("avg_power_mw").over(same_day))
    df = df.withColumn("fleet_stddev_power", F.stddev("avg_power_mw").over(same_day))

    df = df.withColumn("expected_range_lower",
        F.col("fleet_mean_power") - 2 * F.col("fleet_stddev_power"))
    df = df.withColumn("expected_range_upper",
        F.col("fleet_mean_power") + 2 * F.col("fleet_stddev_power"))

    # Std dev is null/0 when a day has a single turbine reading or all
    # turbines report identical output - guard against div-by-zero and
    # treat those days as having no detectable anomaly.
    df = df.withColumn("anomaly_score",
        F.when(F.coalesce(F.col("fleet_stddev_power"), F.lit(0.0)) > 0,
            F.abs(F.col("avg_power_mw") - F.col("fleet_mean_power")) / F.col("fleet_stddev_power")
        ).otherwise(F.lit(0.0))
    )

    df = df.withColumn("is_anomalous",
        (F.coalesce(F.col("fleet_stddev_power"), F.lit(0.0)) > 0) &
        ((F.col("avg_power_mw") < F.col("expected_range_lower")) |
         (F.col("avg_power_mw") > F.col("expected_range_upper")))
    )

    df = df.withColumn("anomaly_reason",
        F.when(~F.col("is_anomalous"), "Normal operation")
        .when(F.col("avg_power_mw") > F.col("expected_range_upper"), "Power output unusually HIGH vs. fleet")
        .otherwise("Power output unusually LOW vs. fleet")
    )

    df = df.withColumn("_ingestion_time", F.current_timestamp())

    result_df = df.select(
        "date",
        "turbine_id",
        "avg_power_mw",
        "fleet_mean_power",
        "fleet_stddev_power",
        "expected_range_lower",
        "expected_range_upper",
        "anomaly_score",
        "is_anomalous",
        "anomaly_reason",
        "record_count",
        "avg_quality_score",
        "_ingestion_time"
    )

    return result_df.orderBy("date", "turbine_id")


@dp.materialized_view(
    name="colibri_assessment.gold.turbine_anomalies",
    comment="Anomaly detection for wind turbine power output vs. same-day fleet baseline",
    table_properties={"quality": "gold"},
    cluster_by=["turbine_id", "date"]
)
def turbine_anomalies():
    """Gold layer: Databricks pipeline entry point, delegates to detect_anomalies."""
    df = spark.read.table("colibri_assessment.gold.turbine_daily_stats")
    return detect_anomalies(df)
