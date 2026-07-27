"""Tests for the read-only repair-planning tools: facts only, alias-based
addressing (never raw model-supplied paths), no write/execute capability."""

from __future__ import annotations

import inspect

import pytest

from src.legacy.repair_tools import ALLOWLISTED_TOOL_NAMES, RepairTools, ToolError, dispatch_tool

DIAGNOSIS = {"diagnosis_status": "DIAGNOSED", "root_cause_category": "ETL_LOGIC", "confidence": "HIGH"}
VALIDATION_RESULTS = {
    "overall_status": "FAIL",
    "checks": [
        {"id": "total_successful_payments_reconciliation", "status": "FAIL"},
        {"id": "loan_count_reconciliation", "status": "PASS"},
    ],
}
BUSINESS_RULES_BY_ALIAS = {
    "STALE": {"successful_payment_statuses": ["PAID"]},
    "ADOPTED": {"successful_payment_statuses": ["PAID", "SETTLED"]},
}
LINEAGE = {"datasets": {"processed.portfolio_summary": {"path": "x", "depends_on": []}}}
ALLOWED_REPAIR_TARGETS = {
    "data/scenarios/x/pipeline_config.json": {"repair_type": "CONFIGURATION_CHANGE", "editable_fields": ["business_rules_file"]}
}
TEST_INVENTORY = ["tests/test_transform.py", "tests/test_validate_portfolio.py"]


@pytest.fixture()
def tools(tmp_path):
    pipeline_config_path = tmp_path / "pipeline_config.json"
    pipeline_config_path.write_text('{"business_rules_file": "context/business_rules.json"}')
    return RepairTools(
        diagnosis=DIAGNOSIS,
        validation_results=VALIDATION_RESULTS,
        business_rules_by_alias=BUSINESS_RULES_BY_ALIAS,
        lineage=LINEAGE,
        pipeline_configuration={"business_rules_file": "context/business_rules.json"},
        allowed_repair_targets=ALLOWED_REPAIR_TARGETS,
        test_inventory=TEST_INVENTORY,
        etl_function_name="compute_portfolio_summary",
        file_hash_paths={"PIPELINE_CONFIG": str(pipeline_config_path)},
    )


def test_get_diagnosis_returns_exact_data(tools):
    assert tools.get_diagnosis() == DIAGNOSIS


def test_get_failed_checks_returns_only_failures(tools):
    result = tools.get_failed_checks()
    assert len(result["failed_checks"]) == 1
    assert result["failed_checks"][0]["id"] == "total_successful_payments_reconciliation"


def test_get_business_rules_by_alias(tools):
    result = tools.get_business_rules("ADOPTED")
    assert result["content"]["successful_payment_statuses"] == ["PAID", "SETTLED"]


def test_get_business_rules_rejects_unknown_alias(tools):
    with pytest.raises(ToolError):
        tools.get_business_rules("NOT_A_REAL_ALIAS")


def test_get_lineage_returns_entry(tools):
    result = tools.get_lineage("total_successful_payments")
    assert result["lineage"]["path"] == "x"


def test_get_pipeline_configuration_when_present(tools):
    result = tools.get_pipeline_configuration()
    assert result["available"] is True
    assert result["business_rules_file"] == "context/business_rules.json"


def test_get_pipeline_configuration_when_absent():
    tools = RepairTools(
        diagnosis=DIAGNOSIS,
        validation_results=VALIDATION_RESULTS,
        business_rules_by_alias=BUSINESS_RULES_BY_ALIAS,
        lineage=LINEAGE,
        pipeline_configuration=None,
        allowed_repair_targets=ALLOWED_REPAIR_TARGETS,
        test_inventory=TEST_INVENTORY,
        etl_function_name="compute_portfolio_summary",
        file_hash_paths={},
    )
    assert tools.get_pipeline_configuration() == {"available": False}


def test_get_relevant_etl_source_returns_real_function_source(tools):
    result = tools.get_relevant_etl_source("total_successful_payments")
    assert result["file"] == "src/transform.py"
    assert "def compute_portfolio_summary" in result["source"]


def test_get_relevant_etl_source_rejects_unknown_function_name():
    tools = RepairTools(
        diagnosis=DIAGNOSIS,
        validation_results=VALIDATION_RESULTS,
        business_rules_by_alias=BUSINESS_RULES_BY_ALIAS,
        lineage=LINEAGE,
        pipeline_configuration=None,
        allowed_repair_targets=ALLOWED_REPAIR_TARGETS,
        test_inventory=TEST_INVENTORY,
        etl_function_name="not_a_real_function",
        file_hash_paths={},
    )
    with pytest.raises(ToolError):
        tools.get_relevant_etl_source("total_successful_payments")


def test_get_allowed_repair_targets_returns_the_full_registry(tools):
    result = tools.get_allowed_repair_targets()
    assert result["targets"] == ALLOWED_REPAIR_TARGETS


def test_get_test_inventory(tools):
    assert tools.get_test_inventory() == {"tests": TEST_INVENTORY}


def test_get_file_hash_by_alias(tools):
    result = tools.get_file_hash("PIPELINE_CONFIG")
    assert result["target_alias"] == "PIPELINE_CONFIG"
    assert len(result["sha256"]) == 64


def test_get_file_hash_rejects_unknown_alias(tools):
    with pytest.raises(ToolError):
        tools.get_file_hash("NOT_A_REAL_ALIAS")


def test_get_file_hash_rejects_missing_file(tools):
    tools.file_hash_paths["MISSING"] = "/definitely/does/not/exist.json"
    with pytest.raises(ToolError):
        tools.get_file_hash("MISSING")


def test_dispatch_tool_returns_error_dict_instead_of_raising(tools):
    result = dispatch_tool(tools, "get_business_rules", {"alias": "NOPE"})
    assert "error" in result


def test_dispatch_tool_rejects_unknown_tool_name(tools):
    result = dispatch_tool(tools, "write_file", {})
    assert "error" in result


def test_no_tool_exposes_write_or_execute_capability():
    for name, _ in inspect.getmembers(RepairTools, predicate=inspect.isfunction):
        if name.startswith("_"):
            continue
        assert name.startswith("get_"), f"unexpected non-getter public method: {name}"


def test_allowlist_matches_actual_tool_methods():
    public_methods = {name for name, _ in inspect.getmembers(RepairTools, predicate=inspect.isfunction) if not name.startswith("_")}
    assert ALLOWLISTED_TOOL_NAMES == public_methods


def test_no_tool_accepts_a_raw_filesystem_path_parameter():
    # Every tool that reads a specific file must address it by alias, never
    # by a parameter literally named/looking like a path.
    for name in ALLOWLISTED_TOOL_NAMES:
        method = getattr(RepairTools, name)
        params = list(inspect.signature(method).parameters)
        for param in params:
            assert "path" not in param.lower(), f"{name} accepts a path-like parameter: {param}"
