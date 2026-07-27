import sys
import types

import pytest


def _install_pipelines_stub() -> None:
    """
    Databricks Lakeflow Declarative Pipelines (`pyspark.pipelines`) only
    exists inside a Databricks pipeline runtime, so it can't be imported in
    a local/OSS PySpark environment. To unit test the transformation
    modules as-is (same files that run in the pipeline, not copies of
    their logic) we register a stand-in module before those files get
    imported. Its decorators are no-ops - they just hand back the wrapped
    function unchanged - which is all that's needed for import-time
    decoration (`@dp.materialized_view(...)`, `@dp.expect(...)`, etc.) to
    succeed. The `spark` global that the decorated entry points reference
    is a separate Databricks-injected object; these tests never call those
    entry point functions directly; they call the pure functions the entry
    points delegate to.
    """
    if "pyspark.pipelines" in sys.modules:
        return

    def _decorator_factory(*_args, **_kwargs):
        def _decorator(func):
            return func
        return _decorator

    stub = types.ModuleType("pyspark.pipelines")
    stub.table = _decorator_factory
    stub.materialized_view = _decorator_factory
    stub.expect = _decorator_factory
    stub.expect_or_fail = _decorator_factory
    stub.expect_or_drop = _decorator_factory
    sys.modules["pyspark.pipelines"] = stub


_install_pipelines_stub()

from pyspark.sql import SparkSession  # noqa: E402  (must follow the stub install)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("wind-turbine-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def data_dir():
    from pathlib import Path
    return Path(__file__).resolve().parent.parent / "data"
