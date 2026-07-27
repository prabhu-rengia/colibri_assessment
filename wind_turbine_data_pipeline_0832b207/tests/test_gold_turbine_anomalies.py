from datetime import date

from transformations.gold.turbine_anomalies import detect_anomalies

DAILY_STATS_COLUMNS = [
    "turbine_id", "date", "min_power_mw", "max_power_mw", "avg_power_mw",
    "std_dev_power_mw", "record_count", "avg_quality_score",
    "pct_high_quality_readings", "power_range_mw",
]


def _daily_stats_row(turbine_id, day, avg_power_mw):
    return (
        turbine_id, day, avg_power_mw, avg_power_mw, avg_power_mw,
        0.1, 24, 1.0, 100.0, 0.0,
    )


def _make_daily_stats_df(spark, rows):
    return spark.createDataFrame(rows, DAILY_STATS_COLUMNS)


def test_flags_turbine_more_than_2_std_dev_from_same_day_fleet_mean(spark):
    day = date(2022, 3, 1)
    # 9 turbines running normally at 2.0 MW, 1 turbine spiking to 100 MW on
    # the same day - the spike should be flagged relative to its peers.
    rows = [_daily_stats_row(i, day, 2.0) for i in range(1, 10)]
    rows.append(_daily_stats_row(10, day, 100.0))

    df = detect_anomalies(_make_daily_stats_df(spark, rows))
    results = {r.turbine_id: r for r in df.collect()}

    assert results[10].is_anomalous is True
    assert results[10].anomaly_reason == "Power output unusually HIGH vs. fleet"
    assert results[1].is_anomalous is False
    assert results[1].anomaly_reason == "Normal operation"


def test_anomaly_detection_is_scoped_to_the_same_day(spark):
    day_1, day_2 = date(2022, 3, 1), date(2022, 3, 2)
    rows = [_daily_stats_row(i, day_1, 2.0) for i in range(1, 10)]
    rows.append(_daily_stats_row(10, day_1, 100.0))
    # A second, unrelated day where every turbine agrees - nothing here
    # should influence or be flagged by day 1's outlier.
    rows += [_daily_stats_row(i, day_2, 3.0) for i in range(1, 6)]

    df = detect_anomalies(_make_daily_stats_df(spark, rows))
    day_2_results = df.filter(df.date == day_2).collect()

    assert all(r.is_anomalous is False for r in day_2_results)


def test_no_anomaly_when_only_one_turbine_reports_that_day(spark):
    day = date(2022, 3, 1)
    rows = [_daily_stats_row(1, day, 2.0)]

    df = detect_anomalies(_make_daily_stats_df(spark, rows))
    row = df.collect()[0]

    assert row.is_anomalous is False
    assert row.anomaly_score == 0.0


def test_low_outlier_flagged_as_unusually_low(spark):
    day = date(2022, 3, 1)
    rows = [_daily_stats_row(i, day, 10.0) for i in range(1, 10)]
    rows.append(_daily_stats_row(10, day, 0.1))

    df = detect_anomalies(_make_daily_stats_df(spark, rows))
    outlier = [r for r in df.collect() if r.turbine_id == 10][0]

    assert outlier.is_anomalous is True
    assert outlier.anomaly_reason == "Power output unusually LOW vs. fleet"
