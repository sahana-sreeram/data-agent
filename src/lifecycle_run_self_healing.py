"""Compose diagnose -> repair-plan -> apply (isolated) -> verify for the loan_portfolio
lifecycle pipeline. Parallel to src/run_self_healing.py (left completely unmodified) for
the S3-backed lifecycle model. Only src/lifecycle_verify_repair.py's deterministic rerun
may mark the outcome VERIFIED and promote it into the real repository/bucket.
"""

from __future__ import annotations

from typing import Callable

from pyspark.sql import SparkSession

from src.lifecycle_apply_repair import run_apply_lifecycle_repair
from src.lifecycle_diagnose_loan_portfolio import run_diagnose_loan_portfolio
from src.lifecycle_verify_repair import run_verify_lifecycle_repair
from src.model_client import DiagnosisModelClient
from src.storage import S3Storage
from src.validate_loan_portfolio import validate_loan_portfolio


def run_lifecycle_self_healing(
    spark: SparkSession,
    storage: S3Storage,
    diagnosis_model_client_factory: Callable[[], DiagnosisModelClient],
    repair_model_client_factory: Callable[[], DiagnosisModelClient],
) -> dict:
    """Diagnose, plan a repair, apply it in isolation, and verify it against real raw data.

    Returns {"diagnosis":..., "repair_plan":..., "repair_result":..., "repair_verification":...}.
    Raises whatever the underlying stages raise (DiagnoseLoanPortfolioError,
    ApplyLifecycleRepairError) on a genuine application-level failure -- a BLOCKED or
    NOT_VERIFIED outcome is a normal, successful return, not an exception.
    """
    business_rules = storage.read_json("context/business_rules.json")
    validation_rules = storage.read_json("context/validations/loan_portfolio.json")
    validation_before = validate_loan_portfolio(storage, business_rules, validation_rules)

    diagnosis = run_diagnose_loan_portfolio(storage, diagnosis_model_client_factory)
    repair_plan, repair_result = run_apply_lifecycle_repair(
        storage, diagnosis, validation_before, repair_model_client_factory
    )
    repair_verification = run_verify_lifecycle_repair(
        spark, storage, business_rules, validation_rules, validation_before, repair_result
    )

    return {
        "diagnosis": diagnosis,
        "repair_plan": repair_plan,
        "repair_result": repair_result,
        "repair_verification": repair_verification,
    }
