"""Tests for src/pr_artifact.py. create_branch=False keeps most tests fast/git-free; one test
confirms the real branch/commit creation path works end to end, cleaning up after itself."""

from __future__ import annotations

import subprocess

import pytest

from src.pr_artifact import LOW_RISK_ROOT_CAUSE_CATEGORIES, build_pr_artifact

DIAGNOSIS = {"root_cause_category": "BUSINESS_RULE_MISMATCH", "root_cause": "hardcoded denominator, ignoring business_rules"}
REPAIR_PLAN = {"change_summary": "read loss_rate_denominator from business_rules instead of hardcoding it"}
VALIDATION_BEFORE = {"checks": [{"id": "loss_rate_reconciliation", "status": "FAIL"}, {"id": "other_check", "status": "PASS"}]}
VALIDATION_AFTER = {"checks": [{"id": "loss_rate_reconciliation", "status": "PASS"}, {"id": "other_check", "status": "PASS"}]}


def _artifact(**overrides):
    kwargs = dict(
        pipeline_name="delinquency_default",
        run_id="run123",
        target_file="src/etl_spark_delinquency_default.py",
        original_content="denominator = 'total_balance_at_default'\n",
        patched_content="denominator = business_rules['loss_rate_denominator']\n",
        diagnosis=DIAGNOSIS,
        repair_plan=REPAIR_PLAN,
        validation_before=VALIDATION_BEFORE,
        validation_after=VALIDATION_AFTER,
        metrics_before={"loss_rate": 0.12},
        metrics_after={"loss_rate": 0.08},
        tests_status={"targeted": "PASS", "full_relevant_suite": "PASS"},
        create_branch=False,
    )
    kwargs.update(overrides)
    return build_pr_artifact(**kwargs)


def test_artifact_contains_a_real_unified_diff():
    artifact = _artifact()
    assert "-denominator = 'total_balance_at_default'" in artifact["diff"]
    assert "+denominator = business_rules['loss_rate_denominator']" in artifact["diff"]
    assert "src/etl_spark_delinquency_default.py" in artifact["diff"]


def test_artifact_reports_failed_checks_before_and_after():
    artifact = _artifact()
    assert artifact["failed_checks_before"] == ["loss_rate_reconciliation"]
    assert artifact["failed_checks_after"] == []


def test_artifact_classifies_business_rule_mismatch_as_low_risk_not_human_review():
    artifact = _artifact()
    assert artifact["risk_classification"] == "LOW"
    assert artifact["human_review_required"] is False


def test_artifact_classifies_source_contract_change_as_high_risk_human_review():
    artifact = _artifact(diagnosis={**DIAGNOSIS, "root_cause_category": "SOURCE_CONTRACT_CHANGE"})
    assert artifact["risk_classification"] == "HIGH"
    assert artifact["human_review_required"] is True


def test_no_branch_created_when_create_branch_is_false():
    artifact = _artifact()
    assert artifact["branch"] is None


def test_artifact_includes_metrics_and_test_status_verbatim():
    artifact = _artifact()
    assert artifact["metrics_before"] == {"loss_rate": 0.12}
    assert artifact["metrics_after"] == {"loss_rate": 0.08}
    assert artifact["tests_status"] == {"targeted": "PASS", "full_relevant_suite": "PASS"}


def test_low_risk_categories_are_exactly_the_auto_repair_eligible_set():
    assert LOW_RISK_ROOT_CAUSE_CATEGORIES == {"ETL_LOGIC", "BUSINESS_RULE_MISMATCH", "DUPLICATION"}


def _real_git_available() -> bool:
    try:
        subprocess.run(["git", "rev-parse", "--is-inside-work-tree"], check=True, capture_output=True, timeout=5)
        return True
    except Exception:  # noqa: BLE001
        return False


@pytest.mark.skipif(not _real_git_available(), reason="not inside a git work tree")
def test_create_branch_true_produces_a_real_inspectable_commit():
    artifact = _artifact(create_branch=True)
    try:
        assert artifact["branch"] is not None
        assert artifact["branch"].startswith("repair/")
        log = subprocess.run(["git", "log", "-1", "--format=%s", artifact["branch"]], capture_output=True, text=True, check=True)
        assert "delinquency_default" in log.stdout
        show = subprocess.run(["git", "show", f"{artifact['branch']}:src/etl_spark_delinquency_default.py"], capture_output=True, text=True, check=True)
        assert show.stdout == "denominator = business_rules['loss_rate_denominator']\n"
    finally:
        if artifact["branch"]:
            subprocess.run(["git", "branch", "-D", artifact["branch"]], capture_output=True)
