from datetime import date, datetime

from transformations.gold.turbine_daily_stats import compute_daily_stats

SILVER_COLUMNS = [
    "timestamp", "date", "turbine_id", "wind_speed", "wind_direction",
    "power_output", "quality_score", "missing_wind_speed", "missing_wind_direction",
]


def _silver_row(ts, turbine_id, power_output, quality_score=1.0):
    ts_dt = datetime.fromisoformat(ts)
    return (
        ts_dt, ts_dt.date(), turbine_id, 10.0, 100,
        power_output, quality_score, False, False,
    )


def _make_silver_df(spark, rows):
    return spark.createDataFrame(rows, SILVER_COLUMNS)


def test_computes_min_max_avg_per_turbine_per_day(spark):
    rows = [
        _silver_row("2022-03-01T00:00:00", 1, 2.0),
        _silver_row("2022-03-01T01:00:00", 1, 4.0),
        _silver_row("2022-03-01T02:00:00", 1, 6.0),
    ]
    df = compute_daily_stats(_make_silver_df(spark, rows))
    row = df.collect()[0]

    assert row.turbine_id == 1
    assert row.date == date(2022, 3, 1)
    assert row.min_power_mw == 2.0
    assert row.max_power_mw == 6.0
    assert row.avg_power_mw == 4.0
    assert row.record_count == 3
    assert row.power_range_mw == 4.0


def test_separates_stats_by_turbine_and_by_day(spark):
    rows = [
        _silver_row("2022-03-01T00:00:00", 1, 2.0),
        _silver_row("2022-03-02T00:00:00", 1, 8.0),  # different day, same turbine
        _silver_row("2022-03-01T00:00:00", 2, 20.0),  # same day, different turbine
    ]
    df = compute_daily_stats(_make_silver_df(spark, rows))

    assert df.count() == 3
    turbine_1_day_1 = df.filter("turbine_id = 1 AND date = '2022-03-01'").collect()[0]
    turbine_1_day_2 = df.filter("turbine_id = 1 AND date = '2022-03-02'").collect()[0]
    assert turbine_1_day_1.avg_power_mw == 2.0
    assert turbine_1_day_2.avg_power_mw == 8.0


def test_quality_metrics_reflect_share_of_high_quality_readings(spark):
    rows = [
        _silver_row("2022-03-01T00:00:00", 1, 2.0, quality_score=1.0),
        _silver_row("2022-03-01T01:00:00", 1, 2.0, quality_score=0.8),
        _silver_row("2022-03-01T02:00:00", 1, 2.0, quality_score=1.0),
        _silver_row("2022-03-01T03:00:00", 1, 2.0, quality_score=1.0),
    ]
    df = compute_daily_stats(_make_silver_df(spark, rows))
    row = df.collect()[0]

    assert row.pct_high_quality_readings == 75.0
