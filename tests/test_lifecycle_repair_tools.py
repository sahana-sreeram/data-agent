"""Tests for the generic, read-only lifecycle repair-planning tools: facts only, no
write/execute capability. Parallel to tests/test_repair_tools.py."""

from __future__ import annotations

import importlib
import inspect

import pytest

from src.etl_spark_loan_portfolio import compute_loan_portfolio
from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY
from src.lifecycle_repair_tools import ALLOWLISTED_TOOL_NAMES, LifecycleRepairTools, ToolError, dispatch_tool

DIAGNOSIS = {"diagnosis_status": "DIAGNOSED", "root_cause_category": "ETL_LOGIC", "confidence": "HIGH"}
VALIDATION_RESULTS = {
    "overall_status": "FAIL",
    "checks": [
        {"id": "loan_count_reconciliation", "status": "FAIL"},
        {"id": "avg_interest_rate_reconciliation", "status": "PASS"},
    ],
}
BUSINESS_RULES = {"successful_payment_statuses": ["PAID"]}
LINEAGE = {"datasets": {"curated.loan_portfolio": {"path": "s3://x/curated/loan_portfolio.parquet", "depends_on": []}}}
METRICS = {"metrics": {"loan_count": {"formula": "count(loans)"}}}
ALLOWED_REPAIR_TARGETS = {
    "src/etl_spark_loan_portfolio.py": {"repair_type": "CODE_CHANGE", "editable_symbols": ["compute_loan_portfolio"]}
}
TEST_INVENTORY = ["tests/test_etl_spark_loan_portfolio.py"]


@pytest.fixture()
def tools():
    return LifecycleRepairTools(
        diagnosis=DIAGNOSIS,
        validation_results=VALIDATION_RESULTS,
        business_rules=BUSINESS_RULES,
        lineage=LINEAGE,
        metrics=METRICS,
        allowed_repair_targets=ALLOWED_REPAIR_TARGETS,
        test_inventory=TEST_INVENTORY,
        lineage_key="curated.loan_portfolio",
        etl_source_file="src/etl_spark_loan_portfolio.py",
        etl_functions={"compute_loan_portfolio": compute_loan_portfolio},
    )


def test_get_diagnosis_returns_exact_data(tools):
    assert tools.get_diagnosis() == DIAGNOSIS


def test_get_failed_checks_returns_only_failures(tools):
    result = tools.get_failed_checks()
    assert len(result["failed_checks"]) == 1
    assert result["failed_checks"][0]["id"] == "loan_count_reconciliation"


def test_get_business_rules_returns_current_content(tools):
    result = tools.get_business_rules()
    assert result["content"] == BUSINESS_RULES


def test_get_lineage_returns_entry(tools):
    result = tools.get_lineage("loan_count")
    assert result["lineage"]["path"] == "s3://x/curated/loan_portfolio.parquet"


def test_get_lineage_rejects_unknown_key():
    tools = LifecycleRepairTools(
        diagnosis=DIAGNOSIS, validation_results=VALIDATION_RESULTS, business_rules=BUSINESS_RULES,
        lineage=LINEAGE, metrics=METRICS, allowed_repair_targets=ALLOWED_REPAIR_TARGETS,
        test_inventory=TEST_INVENTORY, lineage_key="curated.does_not_exist",
    )
    with pytest.raises(ToolError):
        tools.get_lineage("loan_count")


def test_get_pipeline_configuration_always_unavailable(tools):
    assert tools.get_pipeline_configuration() == {"available": False}


def test_get_relevant_etl_source_returns_every_functions_source(tools):
    result = tools.get_relevant_etl_source()
    assert result["file"] == "src/etl_spark_loan_portfolio.py"
    assert set(result["functions"]) == {"compute_loan_portfolio"}
    assert "def compute_loan_portfolio" in result["functions"]["compute_loan_portfolio"]


def test_get_allowed_repair_targets_returns_the_full_registry(tools):
    result = tools.get_allowed_repair_targets()
    assert result["targets"] == ALLOWED_REPAIR_TARGETS


def test_get_test_inventory(tools):
    assert tools.get_test_inventory() == {"tests": TEST_INVENTORY}


def test_get_file_hash_by_alias(tools):
    result = tools.get_file_hash("ETL_SOURCE")
    assert result["target_alias"] == "ETL_SOURCE"
    assert len(result["sha256"]) == 64


def test_get_file_hash_rejects_unknown_alias(tools):
    with pytest.raises(ToolError):
        tools.get_file_hash("NOT_A_REAL_ALIAS")


def test_dispatch_tool_returns_error_dict_instead_of_raising(tools):
    result = dispatch_tool(tools, "get_file_hash", {"target_alias": "NOPE"})
    assert "error" in result


def test_dispatch_tool_rejects_unknown_tool_name(tools):
    result = dispatch_tool(tools, "write_file", {})
    assert "error" in result


def test_no_tool_exposes_write_or_execute_capability():
    for name, _ in inspect.getmembers(LifecycleRepairTools, predicate=inspect.isfunction):
        if name.startswith("_"):
            continue
        assert name.startswith("get_"), f"unexpected non-getter public method: {name}"


def test_allowlist_matches_actual_tool_methods():
    public_methods = {
        name for name, _ in inspect.getmembers(LifecycleRepairTools, predicate=inspect.isfunction) if not name.startswith("_")
    }
    assert ALLOWLISTED_TOOL_NAMES == public_methods


def test_no_tool_accepts_a_raw_filesystem_path_parameter():
    for name in ALLOWLISTED_TOOL_NAMES:
        method = getattr(LifecycleRepairTools, name)
        params = list(inspect.signature(method).parameters)
        for param in params:
            assert "path" not in param.lower(), f"{name} accepts a path-like parameter: {param}"


# --- Generic construction against every registered pipeline's real ETL source -------------


@pytest.mark.parametrize("pipeline_name", list(PIPELINE_REGISTRY))
def test_get_relevant_etl_source_and_file_hash_work_for_every_pipeline(pipeline_name):
    spec = PIPELINE_REGISTRY[pipeline_name]
    module_name = spec.etl_source_file.replace("/", ".").removesuffix(".py")
    etl_module = importlib.import_module(module_name)
    etl_functions = {name: getattr(etl_module, name) for name in spec.etl_function_names}

    tools = LifecycleRepairTools(
        diagnosis=DIAGNOSIS, validation_results=VALIDATION_RESULTS, business_rules=BUSINESS_RULES,
        lineage={"datasets": {spec.lineage_key: {"path": "x", "depends_on": []}}}, metrics=METRICS,
        allowed_repair_targets={spec.etl_source_file: {"repair_type": "CODE_CHANGE"}},
        test_inventory=[spec.test_file], lineage_key=spec.lineage_key,
        etl_source_file=spec.etl_source_file, etl_functions=etl_functions,
    )

    source = tools.get_relevant_etl_source()
    assert source["file"] == spec.etl_source_file
    assert set(source["functions"]) == set(spec.etl_function_names)

    file_hash = tools.get_file_hash("ETL_SOURCE")
    assert len(file_hash["sha256"]) == 64

    lineage_result = tools.get_lineage("some_metric")
    assert lineage_result["lineage"]["path"] == "x"
