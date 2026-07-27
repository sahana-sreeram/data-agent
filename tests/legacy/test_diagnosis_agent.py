"""Tests for the diagnosis agent's tool-calling loop, using a scripted fake model client.

No live model calls anywhere in this file -- ScriptedDiagnosisModelClient
never imports or touches the openai package.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.legacy.diagnosis_agent import SUBMIT_DIAGNOSIS_TOOL_NAME, DiagnosisAgentError, run_diagnosis
from src.legacy.diagnosis_models import DiagnosisValidationError
from src.legacy.diagnostic_tools import DiagnosticTools
from src.model_client import ModelClientError, ModelResponse, ScriptedDiagnosisModelClient, ToolCall

LOANS = [{"loan_id": "L000001", "customer_id": "C000001", "principal_amount": 1000.0, "loan_status": "CLOSED"}]
PAYMENTS = [
    {"payment_id": "P0000001", "loan_id": "L000001", "amount_paid": 500.0, "payment_status": "PAID"},
    {"payment_id": "P0000002", "loan_id": "L000001", "amount_paid": 200.0, "payment_status": "SETTLED"},
]
SUMMARY = {"total_original_principal": 1000.0, "total_successful_payments": 500.0, "total_outstanding_balance": 500.0}
BUSINESS_RULES = {
    "successful_payment_statuses": ["PAID"],
    "valid_payment_statuses": ["PAID", "MISSED", "SCHEDULED", "LATE", "FAILED"],
}
VALIDATION_RESULTS = {
    "overall_status": "FAIL",
    "checks": [{"id": "payment_status_enum_valid", "status": "FAIL", "details": "unexpected values found: ['SETTLED']"}],
}
VALIDATION_RULES = {"tolerance": {"currency": 0.01, "count": 0}, "rules": []}
LINEAGE = {"datasets": {"processed.portfolio_summary": {"path": "x", "depends_on": []}}}
DATA_DICTIONARY = {"portfolio_summary": {"fields": {"total_successful_payments": {"type": "float"}}}}

KNOWN_METRICS = {"total_successful_payments"}
KNOWN_FILES = {"src/transform.py", "context/business_rules.json"}
STARTING_CONTEXT = {"overall_status": "FAIL", "failed_checks": VALIDATION_RESULTS["checks"]}


@pytest.fixture()
def tools():
    return DiagnosticTools(
        loans_df=pd.DataFrame(LOANS),
        payments_df=pd.DataFrame(PAYMENTS),
        portfolio_summary=SUMMARY,
        business_rules=BUSINESS_RULES,
        validation_results=VALIDATION_RESULTS,
        validation_rules=VALIDATION_RULES,
        lineage=LINEAGE,
        data_dictionary=DATA_DICTIONARY,
        pipeline_run=None,
    )


def _run(tools, client, **overrides):
    kwargs = dict(
        known_metric_names=KNOWN_METRICS,
        known_file_paths=KNOWN_FILES,
        validation_overall_status="FAIL",
    )
    kwargs.update(overrides)
    return run_diagnosis(STARTING_CONTEXT, tools, client, **kwargs)


def _submission(diagnosis_status="DIAGNOSED", **overrides):
    payload = {
        "diagnosis_status": diagnosis_status,
        "incident_summary": "SETTLED payments are excluded from successful-payment totals.",
        "affected_metrics": ["total_successful_payments"],
        "root_cause_category": "SOURCE_CONTRACT_CHANGE",
        "initiating_event": None,
        "root_cause": "Raw payments now include SETTLED, which business_rules.json does not recognize as successful.",
        "reasoning_summary": "SETTLED payments resemble PAID payments but are excluded by the current rule.",
        "evidence": [
            {
                "source_type": "RAW_DATA",
                "source_reference": "get_payment_status_counts",
                "finding": "SETTLED appears with nonzero count.",
                "expected": "no SETTLED",
                "actual": "1 SETTLED",
            },
            {
                "source_type": "BUSINESS_RULE",
                "source_reference": "get_business_rules",
                "finding": "successful_payment_statuses only lists PAID.",
                "expected": "PAID recognized",
                "actual": "PAID only, no SETTLED",
            },
        ],
        "recommended_fix": {
            "target_file": "context/business_rules.json",
            "change_summary": "Add SETTLED to successful_payment_statuses if it represents a completed payment.",
            "scope": "MINIMAL",
        },
        "confidence": "MEDIUM",
        "uncertainties": ["Whether SETTLED truly means the payment was received."],
        "additional_evidence_needed": [],
    }
    payload.update(overrides)
    return payload


def test_agent_dispatches_investigation_tools_then_returns_diagnosis(tools):
    responses = [
        ModelResponse(tool_calls=[ToolCall(id="1", name="get_payment_status_counts", arguments={})]),
        ModelResponse(tool_calls=[ToolCall(id="2", name="get_business_rules", arguments={})]),
        ModelResponse(tool_calls=[ToolCall(id="3", name=SUBMIT_DIAGNOSIS_TOOL_NAME, arguments=_submission())]),
    ]
    result = _run(tools, ScriptedDiagnosisModelClient(responses))

    assert result.diagnosis_status.value == "DIAGNOSED"
    assert "total_successful_payments" in result.affected_metrics
    assert len(result.evidence) == 2


def test_agent_allows_zero_investigation_tool_calls_before_submission(tools):
    # The loop doesn't force any particular tool usage -- a model could (unwisely)
    # submit immediately, and the harness must still validate it on its own merits.
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_DIAGNOSIS_TOOL_NAME, arguments=_submission(evidence=[]))])]
    with pytest.raises(DiagnosisValidationError):
        # DIAGNOSED with no evidence must still be rejected.
        _run(tools, ScriptedDiagnosisModelClient(responses))


def test_agent_normalizes_functions_prefixed_source_reference(tools):
    # Real models sometimes echo tool names back as "functions.<name>" (an
    # OpenAI tool-calling convention) inside free-text evidence citations,
    # even though the actual dispatched call had no prefix. This must still
    # be accepted as grounded, with the prefix stripped in the stored result.
    prefixed = _submission(
        evidence=[
            {
                "source_type": "RAW_DATA",
                "source_reference": "functions.get_payment_status_counts",
                "finding": "SETTLED appears with nonzero count.",
                "expected": "no SETTLED",
                "actual": "1 SETTLED",
            }
        ]
    )
    responses = [
        ModelResponse(tool_calls=[ToolCall(id="1", name="get_payment_status_counts", arguments={})]),
        ModelResponse(tool_calls=[ToolCall(id="2", name=SUBMIT_DIAGNOSIS_TOOL_NAME, arguments=prefixed)]),
    ]
    result = _run(tools, ScriptedDiagnosisModelClient(responses))
    assert result.evidence[0].source_reference == "get_payment_status_counts"


def test_agent_rejects_evidence_not_grounded_in_a_real_tool_call(tools):
    ungrounded = _submission()
    ungrounded["evidence"][0]["source_reference"] = "some_tool_never_called"
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_DIAGNOSIS_TOOL_NAME, arguments=ungrounded)])]

    with pytest.raises(DiagnosisValidationError):
        _run(tools, ScriptedDiagnosisModelClient(responses))


def test_agent_rejects_invalid_enum_value(tools):
    bad = _submission(diagnosis_status="MAYBE_KINDA")
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_DIAGNOSIS_TOOL_NAME, arguments=bad)])]

    with pytest.raises(DiagnosisValidationError):
        _run(tools, ScriptedDiagnosisModelClient(responses))


def test_agent_rejects_unknown_affected_metric(tools):
    bad = _submission(affected_metrics=["not_a_real_metric"])
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_DIAGNOSIS_TOOL_NAME, arguments=bad)])]

    with pytest.raises(DiagnosisValidationError):
        _run(tools, ScriptedDiagnosisModelClient(responses))


def test_agent_rejects_fix_target_file_outside_allowlist(tools):
    bad = _submission()
    bad["recommended_fix"]["target_file"] = "/etc/passwd"
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_DIAGNOSIS_TOOL_NAME, arguments=bad)])]

    with pytest.raises(DiagnosisValidationError):
        _run(tools, ScriptedDiagnosisModelClient(responses))


def test_insufficient_evidence_scenario_when_a_tool_call_fails(tools):
    insufficient = _submission(
        diagnosis_status="INSUFFICIENT_EVIDENCE",
        evidence=[],
        confidence="LOW",
        recommended_fix=None,
        additional_evidence_needed=["Could not inspect business rules; need to confirm SETTLED's intended meaning."],
    )
    responses = [
        # Deliberately malformed arguments -> dispatch_tool returns a controlled error, not a crash.
        ModelResponse(tool_calls=[ToolCall(id="1", name="get_business_rules", arguments={"bogus_arg": 1})]),
        ModelResponse(tool_calls=[ToolCall(id="2", name=SUBMIT_DIAGNOSIS_TOOL_NAME, arguments=insufficient)]),
    ]
    result = _run(tools, ScriptedDiagnosisModelClient(responses))

    assert result.diagnosis_status.value == "INSUFFICIENT_EVIDENCE"
    assert result.additional_evidence_needed


def test_insufficient_evidence_requires_a_stated_gap(tools):
    bad = _submission(diagnosis_status="INSUFFICIENT_EVIDENCE", additional_evidence_needed=[])
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_DIAGNOSIS_TOOL_NAME, arguments=bad)])]

    with pytest.raises(DiagnosisValidationError):
        _run(tools, ScriptedDiagnosisModelClient(responses))


def test_no_incident_rejected_when_validation_actually_failed(tools):
    bad = _submission(diagnosis_status="NO_INCIDENT", evidence=[], recommended_fix=None)
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_DIAGNOSIS_TOOL_NAME, arguments=bad)])]

    with pytest.raises(DiagnosisValidationError):
        _run(tools, ScriptedDiagnosisModelClient(responses))


def test_agent_raises_after_max_turns_without_submission(tools):
    responses = [
        ModelResponse(tool_calls=[ToolCall(id=str(i), name="get_business_rules", arguments={})]) for i in range(3)
    ]
    with pytest.raises(DiagnosisAgentError):
        _run(tools, ScriptedDiagnosisModelClient(responses), max_turns=3)


def test_model_client_error_propagates_when_scripted_responses_exhausted(tools):
    with pytest.raises(ModelClientError):
        _run(tools, ScriptedDiagnosisModelClient([]))


def test_initiating_event_can_be_null(tools):
    submission = _submission(initiating_event=None)
    responses = [
        ModelResponse(tool_calls=[ToolCall(id="1", name="get_payment_status_counts", arguments={})]),
        ModelResponse(tool_calls=[ToolCall(id="2", name="get_business_rules", arguments={})]),
        ModelResponse(tool_calls=[ToolCall(id="3", name=SUBMIT_DIAGNOSIS_TOOL_NAME, arguments=submission)]),
    ]
    result = _run(tools, ScriptedDiagnosisModelClient(responses))
    assert result.initiating_event is None


def test_initiating_event_can_be_a_non_empty_string(tools):
    submission = _submission(initiating_event="Business rules were updated to recognize SETTLED.")
    responses = [
        ModelResponse(tool_calls=[ToolCall(id="1", name="get_payment_status_counts", arguments={})]),
        ModelResponse(tool_calls=[ToolCall(id="2", name="get_business_rules", arguments={})]),
        ModelResponse(tool_calls=[ToolCall(id="3", name=SUBMIT_DIAGNOSIS_TOOL_NAME, arguments=submission)]),
    ]
    result = _run(tools, ScriptedDiagnosisModelClient(responses))
    assert result.initiating_event == "Business rules were updated to recognize SETTLED."


def test_initiating_event_rejects_empty_string(tools):
    bad = _submission(initiating_event="   ")
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_DIAGNOSIS_TOOL_NAME, arguments=bad)])]
    with pytest.raises(DiagnosisValidationError):
        _run(tools, ScriptedDiagnosisModelClient(responses))


def test_missing_initiating_event_key_is_rejected(tools):
    bad = _submission()
    del bad["initiating_event"]
    responses = [ModelResponse(tool_calls=[ToolCall(id="1", name=SUBMIT_DIAGNOSIS_TOOL_NAME, arguments=bad)])]
    with pytest.raises(DiagnosisValidationError):
        _run(tools, ScriptedDiagnosisModelClient(responses))


# --- Approved contract change scenario (settled_rule_adopted) -------------
#
# Unlike the settled_bug fixtures above (an unrecognized enum value, no
# reconciliation mismatch), this models the case where SETTLED IS approved
# in the current business rules, but the ETL's last output is stale relative
# to that rule -- so reconciliation checks fail while the enum check passes.
# The agent must distinguish the initiating_event (the approved rule change)
# from the root_cause (the stale ETL output), and must NOT treat
# etl_status=SUCCESS as ruling out an ETL/staleness problem.

RULE_ADOPTED_LOANS = [{"loan_id": "L000001", "customer_id": "C000001", "principal_amount": 1000.0, "loan_status": "CLOSED"}]
RULE_ADOPTED_PAYMENTS = [
    {"payment_id": "P0000001", "loan_id": "L000001", "amount_paid": 500.0, "payment_status": "PAID"},
    {"payment_id": "P0000002", "loan_id": "L000001", "amount_paid": 200.0, "payment_status": "SETTLED"},
]
# The ETL's stale output: computed back when only PAID was recognized.
RULE_ADOPTED_SUMMARY = {
    "total_original_principal": 1000.0,
    "successful_payment_count": 1,
    "total_successful_payments": 500.0,
    "total_outstanding_balance": 500.0,
}
# The CURRENT, approved rule: SETTLED now counts too.
RULE_ADOPTED_BUSINESS_RULES = {
    "successful_payment_statuses": ["PAID", "SETTLED"],
    "valid_payment_statuses": ["PAID", "SETTLED", "MISSED", "SCHEDULED", "LATE", "FAILED"],
}
RULE_ADOPTED_VALIDATION_RESULTS = {
    "overall_status": "FAIL",
    "checks": [
        {
            "id": "successful_payment_count_reconciliation",
            "status": "FAIL",
            "expected": 2,
            "actual": 1,
            "difference": -1,
            "details": None,
        },
        {
            "id": "total_successful_payments_reconciliation",
            "status": "FAIL",
            "expected": 700.0,
            "actual": 500.0,
            "difference": -200.0,
            "details": None,
        },
        {
            "id": "total_outstanding_balance_reconciliation",
            "status": "FAIL",
            "expected": 300.0,
            "actual": 500.0,
            "difference": 200.0,
            "details": None,
        },
    ],
}
RULE_ADOPTED_PIPELINE_RUN = {"etl_status": "SUCCESS", "validation_status": "FAIL", "overall_status": "FAILURE"}
RULE_ADOPTED_LINEAGE = {
    "datasets": {
        "processed.portfolio_summary": {
            "path": "data/processed/portfolio_summary.json",
            "produced_by": "src/transform.py",
            "depends_on": ["raw.loans", "raw.payments", "context.business_rules"],
        }
    }
}
RULE_ADOPTED_DATA_DICTIONARY = {
    "portfolio_summary": {
        "fields": {
            "total_successful_payments": {"type": "float"},
            "total_outstanding_balance": {"type": "float"},
            "successful_payment_count": {"type": "int"},
        }
    }
}
RULE_ADOPTED_KNOWN_METRICS = {"total_successful_payments", "total_outstanding_balance", "successful_payment_count"}
RULE_ADOPTED_KNOWN_FILES = {"src/transform.py", "context/business_rules.json"}
RULE_ADOPTED_STARTING_CONTEXT = {
    "overall_status": "FAIL",
    "failed_checks": RULE_ADOPTED_VALIDATION_RESULTS["checks"],
}


@pytest.fixture()
def rule_adopted_tools():
    return DiagnosticTools(
        loans_df=pd.DataFrame(RULE_ADOPTED_LOANS),
        payments_df=pd.DataFrame(RULE_ADOPTED_PAYMENTS),
        portfolio_summary=RULE_ADOPTED_SUMMARY,
        business_rules=RULE_ADOPTED_BUSINESS_RULES,
        validation_results=RULE_ADOPTED_VALIDATION_RESULTS,
        validation_rules=VALIDATION_RULES,
        lineage=RULE_ADOPTED_LINEAGE,
        data_dictionary=RULE_ADOPTED_DATA_DICTIONARY,
        pipeline_run=RULE_ADOPTED_PIPELINE_RUN,
    )


def test_agent_investigates_lineage_and_etl_source_for_reconciliation_failures(rule_adopted_tools):
    # A plausible investigation path -- NOT forced by the harness, just what
    # this test scripts the fake model to do, to prove the mechanics handle
    # it: consult lineage and ETL source when the failures are reconciliation
    # mismatches rather than an enum violation.
    submission = _submission(
        affected_metrics=["total_successful_payments", "total_outstanding_balance"],
        root_cause_category="ETL_LOGIC",
        initiating_event="Business rules were updated to recognize SETTLED as a successful payment status.",
        root_cause=(
            "The ETL's last output was computed before the rule change and still reflects a PAID-only "
            "successful-payment filter; it has not been rerun against the current business_rules.json."
        ),
        reasoning_summary=(
            "Reconciliation checks fail while the enum check would pass, so this isn't an unrecognized "
            "value problem. get_pipeline_run_metadata shows etl_status SUCCESS, which only means the ETL "
            "executed without error -- it doesn't mean the output matches the current rule. "
            "get_relevant_etl_source shows the filter is driven by business_rules.json's "
            "successful_payment_statuses, which now includes SETTLED, but the stored summary predates that."
        ),
        evidence=[
            {
                "source_type": "VALIDATION",
                "source_reference": "get_failed_checks",
                "finding": "Only reconciliation checks fail; no schema/enum/referential-integrity check fails.",
                "expected": "all checks pass",
                "actual": "3 reconciliation checks fail",
            },
            {
                "source_type": "LINEAGE",
                "source_reference": "get_metric_lineage",
                "finding": "total_successful_payments is produced by src/transform.py from raw.loans/raw.payments and context.business_rules.",
                "expected": None,
                "actual": None,
            },
            {
                "source_type": "ETL_SOURCE",
                "source_reference": "get_relevant_etl_source",
                "finding": "compute_portfolio_summary filters payments via business_rules['successful_payment_statuses'].",
                "expected": None,
                "actual": None,
            },
            {
                "source_type": "BUSINESS_RULE",
                "source_reference": "get_business_rules",
                "finding": "Current successful_payment_statuses includes both PAID and SETTLED.",
                "expected": "ETL output reflects PAID+SETTLED",
                "actual": "ETL output reflects PAID only",
            },
            {
                "source_type": "PIPELINE_METADATA",
                "source_reference": "get_pipeline_run_metadata",
                "finding": "etl_status is SUCCESS despite the output being stale -- execution success does not imply correct output.",
                "expected": None,
                "actual": None,
            },
        ],
        recommended_fix={
            "target_file": "src/transform.py",
            "change_summary": "Rerun the ETL against the current business_rules.json so its output reflects the approved SETTLED rule.",
            "scope": "MINIMAL",
        },
        confidence="HIGH",
    )
    responses = [
        ModelResponse(tool_calls=[ToolCall(id="1", name="get_failed_checks", arguments={})]),
        ModelResponse(tool_calls=[ToolCall(id="2", name="get_metric_lineage", arguments={"metric_name": "total_successful_payments"})]),
        ModelResponse(tool_calls=[ToolCall(id="3", name="get_relevant_etl_source", arguments={"metric_name": "total_successful_payments"})]),
        ModelResponse(tool_calls=[ToolCall(id="4", name="get_business_rules", arguments={})]),
        ModelResponse(tool_calls=[ToolCall(id="5", name="get_pipeline_run_metadata", arguments={})]),
        ModelResponse(tool_calls=[ToolCall(id="6", name=SUBMIT_DIAGNOSIS_TOOL_NAME, arguments=submission)]),
    ]

    result = run_diagnosis(
        RULE_ADOPTED_STARTING_CONTEXT,
        rule_adopted_tools,
        ScriptedDiagnosisModelClient(responses),
        known_metric_names=RULE_ADOPTED_KNOWN_METRICS,
        known_file_paths=RULE_ADOPTED_KNOWN_FILES,
        validation_overall_status="FAIL",
    )

    assert result.diagnosis_status.value == "DIAGNOSED"
    assert set(result.affected_metrics) == {"total_successful_payments", "total_outstanding_balance"}
    assert result.initiating_event is not None and "rule" in result.initiating_event.lower()
    assert "stale" in result.root_cause.lower() or "not been rerun" in result.root_cause.lower()
    assert result.confidence.value == "HIGH"


def test_agent_handles_multiple_tool_calls_in_a_single_turn(tools):
    responses = [
        ModelResponse(
            tool_calls=[
                ToolCall(id="1", name="get_payment_status_counts", arguments={}),
                ToolCall(id="2", name="get_business_rules", arguments={}),
            ]
        ),
        ModelResponse(tool_calls=[ToolCall(id="3", name=SUBMIT_DIAGNOSIS_TOOL_NAME, arguments=_submission())]),
    ]
    result = _run(tools, ScriptedDiagnosisModelClient(responses))
    assert result.diagnosis_status.value == "DIAGNOSED"
