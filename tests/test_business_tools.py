"""Tests for the business Q&A agent's read-only tools.

Covers: tool dispatch happy paths, unknown-metric/unknown-tool/bad-argument
handling (must return an {"error": ...} dict, never raise), and the
no-write/no-execute safety guarantee shared with diagnostic_tools.py.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

from src.business_tools import ALLOWLISTED_TOOL_NAMES, TOOL_SPECS, BusinessTools, ToolError, dispatch_tool

PORTFOLIO_SUMMARY = {
    "loan_count": 10,
    "total_outstanding_balance": 997522.36,
}
BUSINESS_RULES = {"successful_payment_statuses": ["PAID"]}
DATA_DICTIONARY = {
    "portfolio_summary": {
        "fields": {
            "total_outstanding_balance": {"type": "float", "description": "principal minus successful payments"},
        }
    }
}


def _tools() -> BusinessTools:
    return BusinessTools(portfolio_summary=PORTFOLIO_SUMMARY, business_rules=BUSINESS_RULES, data_dictionary=DATA_DICTIONARY)


def test_get_portfolio_summary_returns_the_full_trusted_summary():
    result = dispatch_tool(_tools(), "get_portfolio_summary", {})
    assert result == PORTFOLIO_SUMMARY


def test_get_metric_definition_returns_known_field():
    result = dispatch_tool(_tools(), "get_metric_definition", {"metric_name": "total_outstanding_balance"})
    assert result == {"total_outstanding_balance": DATA_DICTIONARY["portfolio_summary"]["fields"]["total_outstanding_balance"]}


def test_get_metric_definition_unknown_metric_returns_error_not_raise():
    result = dispatch_tool(_tools(), "get_metric_definition", {"metric_name": "not_a_real_metric"})
    assert "error" in result
    assert "not_a_real_metric" in result["error"]


def test_get_business_rules_returns_the_approved_rules():
    result = dispatch_tool(_tools(), "get_business_rules", {})
    assert result == BUSINESS_RULES


def test_dispatch_unknown_tool_name_returns_error_not_raise():
    result = dispatch_tool(_tools(), "delete_everything", {})
    assert "error" in result


def test_dispatch_bad_arguments_returns_error_not_raise():
    result = dispatch_tool(_tools(), "get_portfolio_summary", {"unexpected_arg": 1})
    assert "error" in result


def test_tool_error_raised_directly_is_a_normal_exception():
    try:
        _tools().get_metric_definition("nope")
    except ToolError as exc:
        assert "nope" in str(exc)
    else:
        raise AssertionError("expected ToolError")


def test_tool_specs_cover_every_allowlisted_tool_name():
    spec_names = {spec["function"]["name"] for spec in TOOL_SPECS}
    assert spec_names == set(ALLOWLISTED_TOOL_NAMES)


def test_no_tool_exposes_write_or_execute_capability():
    write_like_keywords = ("write", "delete", "exec", "eval", "subprocess", "system", "patch", "apply")
    for name in ALLOWLISTED_TOOL_NAMES:
        assert not any(kw in name.lower() for kw in write_like_keywords), name

    for name, _ in inspect.getmembers(BusinessTools, predicate=inspect.isfunction):
        if name.startswith("_"):
            continue
        assert not any(kw in name.lower() for kw in write_like_keywords), name


def test_no_subprocess_usage_in_business_tools_module():
    tree = ast.parse(Path("src/business_tools.py").read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert not any(alias.name == "subprocess" for alias in node.names)
        if isinstance(node, ast.ImportFrom):
            assert node.module != "subprocess"
