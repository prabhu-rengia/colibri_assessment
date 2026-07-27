# Wind Turbine Data Pipeline

A Databricks **Lakeflow Declarative Pipelines** (formerly Delta Live Tables)
pipeline that ingests daily turbine CSVs, cleans them, computes summary
statistics, flags anomalies, and stores the results as Delta tables for
further analysis.

## Design

Medallion architecture, one file per table:

```
transformations/
  bronze/turbine_raw.py        Auto Loader ingest of the raw CSVs, CDC-merged
  silver/turbine_cleaned.py    dedup, missing-value imputation, outlier removal
  gold/turbine_daily_stats.py  per-turbine, per-day min/max/avg/stddev
  gold/turbine_anomalies.py    per-turbine, per-day anomaly flag
```

Each layer reads from the previous one as a Delta table
(`colibri_assessment.bronze/silver/gold.*`) and writes its own Delta table -
that's the "database" the processed data and summary statistics land in,
queryable directly from Databricks SQL/notebooks once the pipeline is
deployed.

Each transformation file separates **pure logic** from the Databricks
pipeline glue:

- `clean_turbine_data(raw_df)`, `compute_daily_stats(cleaned_df)`,
  `detect_anomalies(daily_stats_df)` are plain functions of a DataFrame to a
  DataFrame - no dependency on Databricks-only APIs.
- The `@dp.materialized_view`-decorated function in each Silver/Gold file is
  a thin entry point: it reads the upstream table and calls the pure
  function. That's the only part of each file (besides bronze, which is
  pure I/O) that can't be exercised outside a Databricks pipeline runtime.

This split is what makes the pipeline testable: `tests/` runs the pure
functions against a local PySpark session and the sample CSVs, with no
Databricks connection required.

### Ingestion (Bronze)

Auto Loader streams the raw CSVs in, but doesn't write them straight to the
bronze table with a plain append. Instead, `create_auto_cdc_flow` merges
the stream into the target table, upserting on `(turbine_id, timestamp)`
(see Assumptions below for why a plain streaming append doesn't fit this
source's update pattern). That keeps bronze exactly in sync with the
source CSVs' current contents, however many times a file gets reprocessed.

This fixes *storage*, not *compute*: `allowOverwrites` still makes Auto
Loader re-read and re-emit a changed file's full current contents on every
change (CSV can't be read incrementally by offset), so the merge's input
volume still grows the same way bronze storage used to - the CDC upsert
just discards the resulting duplicates on write instead of persisting them.
Acceptable at this data volume; would need revisiting for large per-file
sizes.

### Cleaning (Silver)

- Deduplicates on `(timestamp, turbine_id)` - necessary because re-ingesting
  an updated source file (see Assumptions) can re-emit rows already seen.
- Missing `wind_speed` / `wind_direction` are imputed with that turbine's
  own median. Missing `power_output` is dropped
  (`@dp.expect_or_drop`) rather than imputed, since it's the primary
  measurement being reported on and a made-up value could hide a real
  sensor fault.
- `power_output` outliers are removed per turbine using a 3-sigma rule
  (fixed 3 standard deviations from that turbine's own mean, distinct from
  the 2-standard-deviation rule used for anomaly detection below).
- Each row gets a `quality_score` (1.0 = no imputation needed, 0.8 = one or
  both wind sensors imputed) that rolls up into the daily stats.

### Summary statistics (Gold)

`turbine_daily_stats` aggregates the cleaned data to one row per
`(turbine_id, date)`: min/max/avg/stddev of `power_output`, record count,
and data-quality metrics. "Date" is the 24-hour grouping window called for
in the brief.

### Anomaly detection (Gold)

The brief defines an anomaly as a turbine whose output is "outside of 2
standard deviations from the mean... over the same time period." That's
implemented literally: for each date, `turbine_anomalies` computes the
**fleet's** mean and standard deviation of `avg_power_mw` across all
turbines reporting that day, and flags any turbine whose value falls
outside `fleet_mean ± 2 * fleet_stddev`.

This was a deliberate choice over the alternative reading (comparing a
turbine to its *own* multi-day rolling history): turbines on the same farm
see essentially the same wind, so a turbine that's an outlier relative to
its peers on a given day is a meaningful signal (sensor fault, mechanical
issue, curtailment), and it works from day one without needing several
days of backfilled history first. The trade-off is that a farm-wide event
(e.g. a storm affecting every turbine equally) won't be flagged, since nothing
looks anomalous *relative to its peers* - a rolling-baseline approach would
catch that case instead, at the cost of needing history to warm up. Days
with only one turbine reporting (undefined stddev) are treated as having no
detectable anomaly rather than raising a division-by-zero error.

## Assumptions

- **Source files are updated in place, not replaced.** The brief states
  each turbine's CSV is appended with the last 24h of readings daily - the
  same file grows over time rather than a new file landing each day. Auto
  Loader's default file-discovery mode reads a given file once, so bronze
  sets `cloudFiles.allowOverwrites = "true"` to detect and reprocess
  modified files. CSV has no byte-offset resume point, so that reprocessing
  re-emits every row already in the file, not just the newly appended ones
  - a plain append into bronze (`@dp.table`) would therefore make bronze
  grow by the *cumulative* size of every daily snapshot (e.g. a 100-row
  file that grows to 210 then 230 rows would leave bronze holding
  100 + 210 + 230 = 540 rows, not 230). To avoid that, bronze uses
  `create_auto_cdc_flow` to merge the stream into the target table keyed on
  `(turbine_id, timestamp)` instead: re-emitted rows upsert in place rather
  than piling up, so bronze always matches the source's current row count.
  The Silver-layer dedup on `(timestamp, turbine_id)` is kept regardless, as
  a cheap safeguard against duplicate rows from any source, not just this
  one.
- **The pipeline is triggered (not continuous) and runs on a schedule that
  follows each day's CSV update** (e.g. a daily Databricks Job). Bronze
  reflects the source only as of the last pipeline run, not in real time.
- **Turbine IDs map 1:1 to a fixed CSV file** (`turbine 1` always in
  `data_group_1.csv`, etc.), per the brief - no cross-file join or
  reconciliation is needed to resolve a turbine's identity.
- **"Missing data" from sensor malfunctions shows up as null fields within
  a row** (e.g. `wind_speed`/`wind_direction` present but null), not as
  entirely absent rows for a given hour - there's no reliable way to detect
  a row that was never written at all from the CSV alone (no fixed-cadence
  contract is enforced upstream).
- **A missing `power_output` reading is dropped, not imputed** - see
  Cleaning above.
- **Outlier and anomaly thresholds use sample standard deviation**
  (Spark's default `stddev`), consistent with treating each turbine's
  (or, for anomalies, each day's fleet) readings as a sample.

## Known limitations / not yet done

- No Databricks Asset Bundle / pipeline deployment config (`databricks.yml`)
  is included yet - this repo currently defines the transformations only.
- Bronze (`turbine_raw.py`) is a thin Auto Loader + CDC-merge I/O wrapper
  and isn't covered by the local test suite, since Auto Loader and
  `create_auto_cdc_flow` only run inside a Databricks pipeline; it's
  exercised implicitly by whatever integration/staging environment the
  pipeline is deployed to.

## Running the tests

```bash
python -m pip install -r requirements-dev.txt
python -m pytest
```

Tests spin up a local, single-machine PySpark session (no Databricks
connection needed) and run the Silver/Gold pure functions against small,
purpose-built DataFrames plus the full month of sample data in `data/`.

`pyspark.pipelines` (Databricks-only) is stubbed out in `tests/conftest.py`
with no-op decorators so the production files can be imported and tested
as-is.
