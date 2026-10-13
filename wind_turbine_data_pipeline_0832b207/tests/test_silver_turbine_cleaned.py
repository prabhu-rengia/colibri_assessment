from datetime import datetime

from transformations.silver.turbine_cleaned import clean_turbine_data, extract_outliers


def _bronze_row(ts, turbine_id, wind_speed, wind_direction, power_output):
    return (
        datetime.fromisoformat(ts), turbine_id, wind_speed, wind_direction,
        power_output, "data_group_1.csv", datetime.fromisoformat(ts)
    )


BRONZE_COLUMNS = [
    "timestamp", "turbine_id", "wind_speed", "wind_direction",
    "power_output", "_source_file", "_ingestion_time"
]


def _make_bronze_df(spark, rows):
    return spark.createDataFrame(rows, BRONZE_COLUMNS)


def test_imputes_missing_wind_values_with_turbine_median(spark):
    rows = [
        _bronze_row("2022-03-01T00:00:00", 1, 10.0, 100, 2.0),
        _bronze_row("2022-03-01T01:00:00", 1, 20.0, 200, 2.0),
        _bronze_row("2022-03-01T02:00:00", 1, None, None, 2.0),  # missing sensor readings
    ]
    df = clean_turbine_data(_make_bronze_df(spark, rows))
    imputed = df.filter("timestamp = '2022-03-01T02:00:00'").collect()[0]

    assert imputed.wind_speed == 10.0  # percentile_approx median of [10, 20] -> 10
    assert imputed.wind_direction == 100
    assert imputed.missing_wind_speed is True
    assert imputed.missing_wind_direction is True
    assert imputed.quality_score == 0.8


def test_full_reading_gets_perfect_quality_score(spark):
    rows = [_bronze_row("2022-03-01T00:00:00", 1, 10.0, 100, 2.0)]
    df = clean_turbine_data(_make_bronze_df(spark, rows))
    row = df.collect()[0]

    assert row.missing_wind_speed is False
    assert row.missing_wind_direction is False
    assert row.quality_score == 1.0


def test_deduplicates_on_timestamp_and_turbine_id(spark):
    rows = [
        _bronze_row("2022-03-01T00:00:00", 1, 10.0, 100, 2.0),
        _bronze_row("2022-03-01T00:00:00", 1, 10.0, 100, 2.0),  # re-ingested duplicate
        _bronze_row("2022-03-01T00:00:00", 2, 11.0, 110, 2.5),  # same timestamp, different turbine
    ]
    df = clean_turbine_data(_make_bronze_df(spark, rows))

    assert df.count() == 2


def test_removes_power_output_outliers_using_3_sigma_rule(spark):
    # Build a stable baseline of readings for turbine 1, then inject one
    # wildly out-of-range value that should be dropped as an outlier.
    rows = [
        _bronze_row(f"2022-03-01T{h:02d}:00:00", 1, 10.0, 100, 2.0)
        for h in range(10)
    ]
    rows.append(_bronze_row("2022-03-01T23:00:00", 1, 10.0, 100, 500.0))

    df = clean_turbine_data(_make_bronze_df(spark, rows))
    powers = sorted(r.power_output for r in df.collect())

    assert 500.0 not in powers
    assert df.count() == 10


def test_outlier_removal_is_scoped_per_turbine(spark):
    # Turbine 1 normally runs ~2 MW, turbine 2 normally runs ~4 MW. Neither
    # is an outlier relative to its own history, even though turbine 2's
    # baseline would look like an outlier against turbine 1's.
    rows = [
        _bronze_row(f"2022-03-01T{h:02d}:00:00", 1, 10.0, 100, 2.0)
        for h in range(5)
    ] + [
        _bronze_row(f"2022-03-01T{h:02d}:00:00", 2, 10.0, 100, 4.0)
        for h in range(5)
    ]

    bronze = _make_bronze_df(spark, rows)

    assert clean_turbine_data(bronze).count() == 10
    assert extract_outliers(bronze).count() == 0


def test_outliers_are_stored_with_the_stats_they_were_judged_against(spark):
    rows = [
        _bronze_row(f"2022-03-01T{h:02d}:00:00", 1, 10.0, 100, 2.0)
        for h in range(10)
    ]
    rows.append(_bronze_row("2022-03-01T23:00:00", 1, 10.0, 100, 500.0))

    outliers = extract_outliers(_make_bronze_df(spark, rows)).collect()

    assert len(outliers) == 1
    assert outliers[0].power_output == 500.0
    assert outliers[0].turbine_mean_power > 2.0
    assert outliers[0].turbine_stddev_power > 0.0


def test_every_reading_lands_in_exactly_one_of_cleaned_or_outliers(spark):
    rows = [
        _bronze_row(f"2022-03-01T{h:02d}:00:00", 1, 10.0, 100, 2.0)
        for h in range(10)
    ]
    rows.append(_bronze_row("2022-03-01T23:00:00", 1, 10.0, 100, 500.0))
    bronze = _make_bronze_df(spark, rows)

    cleaned_ts = {r.timestamp for r in clean_turbine_data(bronze).collect()}
    outlier_ts = {r.timestamp for r in extract_outliers(bronze).collect()}

    assert cleaned_ts.isdisjoint(outlier_ts)
    assert len(cleaned_ts | outlier_ts) == 11
