"""Tests for src/run_lifecycle_etl_pipelines.py against the REAL migrated
s3://<bucket>/raw/ data and a REAL Spark session. Skips cleanly if either is
unavailable in this environment.
"""

from __future__ import annotations

import pytest

from src.run_lifecycle_etl_pipelines import PIPELINES, run_all_pipelines


@pytest.fixture
def real_data_present(s3_storage):
    if not s3_storage.exists("raw/loans.parquet"):
        pytest.skip("data/lifecycle/raw/ not migrated to S3 in this environment")


def test_all_five_pipelines_run_and_validate_against_real_data(spark_session, s3_storage, real_data_present):
    run_record = run_all_pipelines(spark_session, s3_storage)

    assert set(run_record["pipelines"]) == set(PIPELINES)
    for name, result in run_record["pipelines"].items():
        assert result["etl_status"] == "SUCCESS", f"{name}: {result['etl_error']}"
        assert result["validation_status"] == "PASS", f"{name} failed validation"
    assert run_record["overall_status"] == "SUCCESS"


def test_curated_outputs_all_exist_after_run(spark_session, s3_storage, real_data_present):
    run_all_pipelines(spark_session, s3_storage)

    expected_keys = [
        "curated/loan_portfolio.parquet",
        "curated/campaign_funnel.parquet",
        "curated/underwriting_performance.parquet",
        "curated/underwriting_performance_rejections.parquet",
        "curated/payment_performance.parquet",
        "curated/delinquency_default.parquet",
    ]
    for key in expected_keys:
        assert s3_storage.exists(key), f"missing curated output: {key}"


def test_isolated_etl_failure_does_not_block_the_others(spark_session, s3_storage, real_data_present, monkeypatch):
    import src.run_lifecycle_etl_pipelines as orchestrator

    def _broken_etl(*args, **kwargs):
        raise RuntimeError("simulated ETL failure")

    original_validate = orchestrator.PIPELINES["loan_portfolio"][1]
    monkeypatch.setitem(orchestrator.PIPELINES, "loan_portfolio", (_broken_etl, original_validate))

    run_record = run_all_pipelines(spark_session, s3_storage)

    assert run_record["pipelines"]["loan_portfolio"]["etl_status"] == "FAILURE"
    assert "simulated ETL failure" in run_record["pipelines"]["loan_portfolio"]["etl_error"]
    assert run_record["pipelines"]["loan_portfolio"]["validation_status"] == "NOT_RUN"
    assert run_record["pipelines"]["campaign_funnel"]["etl_status"] == "SUCCESS"
    assert run_record["overall_status"] == "FAILURE"


def test_isolated_validation_failure_is_attributed_to_validation_not_etl(spark_session, s3_storage, real_data_present, monkeypatch):
    import src.run_lifecycle_etl_pipelines as orchestrator

    def _broken_validate(*args, **kwargs):
        raise RuntimeError("simulated validator crash")

    original_etl = orchestrator.PIPELINES["loan_portfolio"][0]
    monkeypatch.setitem(orchestrator.PIPELINES, "loan_portfolio", (original_etl, _broken_validate))

    run_record = run_all_pipelines(spark_session, s3_storage)

    result = run_record["pipelines"]["loan_portfolio"]
    # The ETL itself succeeded -- only the validator crashed -- so etl_status must say so,
    # not be misreported as a FAILURE the way a single shared try/except used to.
    assert result["etl_status"] == "SUCCESS"
    assert result["etl_error"] is None
    assert result["validation_status"] == "ERROR"
    assert "simulated validator crash" in result["validation_error"]
    assert run_record["overall_status"] == "FAILURE"
