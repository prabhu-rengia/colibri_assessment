from transformations.silver.turbine_cleaned import clean_turbine_data
from transformations.gold.turbine_daily_stats import compute_daily_stats
from transformations.gold.turbine_anomalies import detect_anomalies


def _read_group_as_bronze(spark, data_dir, filename):
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(str(data_dir / filename))
    )
    return (
        df.withColumn("_source_file", df["turbine_id"].cast("string"))
        .withColumn("_ingestion_time", df["timestamp"])
    )


def test_full_month_of_clean_sample_data_flows_through_all_layers(spark, data_dir):
    bronze = _read_group_as_bronze(spark, data_dir, "data_group_1.csv")
    silver = clean_turbine_data(bronze)
    daily_stats = compute_daily_stats(silver)
    anomalies = detect_anomalies(daily_stats)

    # 5 turbines x 31 days x 24 hours, sample data has no gaps or outliers.
    assert silver.count() == 5 * 31 * 24
    assert daily_stats.count() == 5 * 31

    stats_row = daily_stats.collect()[0]
    assert stats_row.min_power_mw <= stats_row.avg_power_mw <= stats_row.max_power_mw

    # Anomaly output must cover every (turbine, day) that daily stats has.
    assert anomalies.count() == daily_stats.count()
    assert {r.is_anomalous for r in anomalies.collect()} <= {True, False}


def test_all_three_turbine_groups_are_structurally_consistent(spark, data_dir):
    for filename in ["data_group_1.csv", "data_group_2.csv", "data_group_3.csv"]:
        bronze = _read_group_as_bronze(spark, data_dir, filename)
        silver = clean_turbine_data(bronze)
        daily_stats = compute_daily_stats(silver)

        assert silver.count() == 5 * 31 * 24
        assert daily_stats.filter(daily_stats.record_count != 24).count() == 0
