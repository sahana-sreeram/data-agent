"""Fixed catalog of injectable bugs and refusal-accuracy cases used by src/eval_harness.py
to score the lifecycle self-healing pipeline against real, known-shape failures.

Each BugScenario is a single, surgical find/replace against one of the 5 real lifecycle ETL
source files -- the exact same two bugs already proven live this session
(loan_portfolio's inner-join, delinquency_default's hardcoded loss_rate denominator) plus two
new ones in different pipelines, chosen to give a genuine 2x2 (bug_class x pipeline) matrix
rather than just re-running the same two forever. `find` is asserted (by
tests/test_eval_scenarios.py) to occur exactly once in the current file content, so a future
refactor of these files that silently invalidates a scenario fails loudly in the test suite
rather than producing a confusing eval run.

REFUSAL_CASES exercises src.repair_models.evaluate_repair_eligibility directly -- the actual,
deterministic (non-LLM) gate that decides whether an incident may even reach the repair
model -- with a mix of cases that SHOULD be refused (HUMAN_REVIEW_REQUIRED) and one that
should NOT, so a harness that always refuses can't trivially score 100%.
"""

from __future__ import annotations

from dataclasses import dataclass

from src.repair_models import RepairEligibility


@dataclass(frozen=True)
class BugScenario:
    name: str
    pipeline_name: str
    bug_class: str  # "ETL_LOGIC_JOIN" | "BUSINESS_RULE_MISMATCH"
    target_file: str
    find: str
    replace: str
    expected_root_cause_category: str
    description: str


@dataclass(frozen=True)
class UpstreamContractScenario:
    """A genuine upstream contract change, not a source-code bug: the injected difference is
    in the DATA payment_service produces, not in any ETL file. src.eval_harness's
    run_upstream_contract_scenario regenerates raw/payment_schedule.parquet and
    raw/payment_events.parquet for real, through the real payment_service running at
    `contract_version` and the real events_to_lifecycle_tables adapter -- src/etl_spark_*.py
    is never touched for this scenario, only the raw data it reads."""

    name: str
    pipeline_name: str
    contract_version: str
    num_customers: int
    seed: int
    expected_root_cause_category: str
    description: str


UPSTREAM_CONTRACT_SCENARIOS: list[UpstreamContractScenario] = [
    UpstreamContractScenario(
        name="payment_service_v2_settled_rename",
        pipeline_name="loan_portfolio",
        contract_version="v2",
        num_customers=300,
        seed=42,
        expected_root_cause_category="SOURCE_CONTRACT_CHANGE",
        description=(
            "payment_service v2 renames a successfully collected installment's payment_status "
            "from PAID to SETTLED -- a real upstream contract change, not a bug. "
            "loan_portfolio's ETL only recognizes PAID as successful, so net_paid silently drops "
            "to ~0 and total_outstanding_principal is overstated once payment_service runs at v2. "
            "This produces two independent, corroborating signals: (1) the raw 12-table validator's "
            "payment_events.payment_status enum check fails outright (SETTLED isn't an approved "
            "value), and (2) validate_loan_portfolio.py's total_outstanding_principal_status_"
            "vocabulary_drift check fails because a payment_status-label-agnostic recomputation "
            "(amount field only, immune to the rename) disagrees with the business-rule-driven "
            "curated value by far more than the tolerance LATE payments alone would ever explain. "
            "Per policy, SOURCE_CONTRACT_CHANGE is never auto-repaired regardless of diagnosis "
            "confidence -- evaluate_repair_eligibility routes it to HUMAN_REVIEW_REQUIRED."
        ),
    )
]


BUG_SCENARIOS: list[BugScenario] = [
    BugScenario(
        name="loan_portfolio_inner_join",
        pipeline_name="loan_portfolio",
        bug_class="ETL_LOGIC_JOIN",
        target_file="src/etl_spark_loan_portfolio.py",
        find='joined = loans.join(net_paid_by_loan, on="loan_id", how="left").fillna({"net_paid": 0.0})',
        replace='joined = loans.join(net_paid_by_loan, on="loan_id", how="inner").fillna({"net_paid": 0.0})',
        expected_root_cause_category="ETL_LOGIC",
        description="Switches the net-payment join to an inner join, silently dropping every loan with zero PAID/REVERSED payment activity from the whole portfolio.",
    ),
    BugScenario(
        name="delinquency_default_hardcoded_denominator",
        pipeline_name="delinquency_default",
        bug_class="BUSINESS_RULE_MISMATCH",
        target_file="src/etl_spark_delinquency_default.py",
        find='loss_denominator_column = business_rules.get("loss_rate_denominator", "total_funded_principal")',
        replace='loss_denominator_column = "total_balance_at_default"',
        expected_root_cause_category="BUSINESS_RULE_MISMATCH",
        description="Hardcodes loss_rate's denominator column, ignoring business_rules.loss_rate_denominator entirely.",
    ),
    BugScenario(
        name="payment_performance_hardcoded_threshold",
        pipeline_name="payment_performance",
        bug_class="BUSINESS_RULE_MISMATCH",
        target_file="src/etl_spark_payment_performance.py",
        find='threshold_days = business_rules["prepayment_threshold_days"]',
        replace="threshold_days = 10",
        expected_root_cause_category="BUSINESS_RULE_MISMATCH",
        description="Hardcodes the prepayment threshold to 10 days, ignoring business_rules.prepayment_threshold_days.",
    ),
    BugScenario(
        name="campaign_funnel_raw_null_join",
        pipeline_name="campaign_funnel",
        bug_class="ETL_LOGIC_JOIN",
        target_file="src/etl_spark_campaign_funnel.py",
        find='prequal_offers.withColumn("campaign_key", _sentinel_for_null("campaign_id"))',
        replace='prequal_offers.withColumn("campaign_key", F.col("campaign_id"))',
        expected_root_cause_category="ETL_LOGIC",
        description=(
            "Keys offers_by_campaign's aggregation on the raw (nullable) campaign_id instead of the "
            "organic sentinel -- Spark's equi-join treats NULL != NULL, so the organic row's "
            "offers_created (and everything derived from it) silently drops to 0."
        ),
    ),
]


def _diagnosis(**overrides) -> dict:
    base = {
        "diagnosis_status": "DIAGNOSED",
        "root_cause_category": "BUSINESS_RULE_MISMATCH",
        "confidence": "HIGH",
        "recommended_fix": {"target_file": "src/etl_spark_loan_portfolio.py", "change_summary": "x", "scope": "MINIMAL"},
        "evidence": [{"source_type": "ETL_SOURCE", "source_reference": "get_relevant_etl_source", "finding": "x", "expected": None, "actual": None}],
    }
    base.update(overrides)
    return base


REFUSAL_CASES: list[tuple] = [
    (
        "low_confidence_should_refuse",
        _diagnosis(confidence="LOW"),
        {"src/etl_spark_loan_portfolio.py"},
        RepairEligibility.HUMAN_REVIEW_REQUIRED,
    ),
    (
        "source_contract_change_should_refuse",
        _diagnosis(root_cause_category="SOURCE_CONTRACT_CHANGE"),
        {"src/etl_spark_loan_portfolio.py"},
        RepairEligibility.HUMAN_REVIEW_REQUIRED,
    ),
    (
        "insufficient_evidence_should_refuse",
        {"diagnosis_status": "INSUFFICIENT_EVIDENCE", "root_cause_category": None, "confidence": None, "recommended_fix": None, "evidence": []},
        {"src/etl_spark_loan_portfolio.py"},
        RepairEligibility.HUMAN_REVIEW_REQUIRED,
    ),
    (
        "no_incident_is_a_clean_noop_not_a_refusal",
        {"diagnosis_status": "NO_INCIDENT", "root_cause_category": None, "confidence": None, "recommended_fix": None, "evidence": []},
        {"src/etl_spark_loan_portfolio.py"},
        RepairEligibility.NO_REPAIR_NEEDED,
    ),
    (
        "ungrounded_target_file_should_refuse",
        _diagnosis(recommended_fix={"target_file": "src/some_unregistered_file.py", "change_summary": "x", "scope": "MINIMAL"}),
        {"src/etl_spark_loan_portfolio.py"},
        RepairEligibility.HUMAN_REVIEW_REQUIRED,
    ),
    (
        "fully_valid_diagnosis_should_proceed",
        _diagnosis(),
        {"src/etl_spark_loan_portfolio.py"},
        RepairEligibility.ELIGIBLE_FOR_REPAIR,
    ),
]
