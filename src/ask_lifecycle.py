"""Answer a business question from the 5 curated lifecycle pipeline outputs in S3.

Deliberately simpler than src/ask.py: there is no diagnose/repair/verify loop
for this model yet (no failure-injection scenario has been built on top of it),
so this only does the "detect a bad answer, refuse rather than fabricate"
half of ask.py's contract, not the auto-heal half. It reads
s3://<bucket>/curated/pipeline_run.json (written by
src/run_lifecycle_etl_pipelines.py) and refuses to answer if any pipeline's
last run didn't fully succeed and validate -- exactly the same "don't answer
from data known to be wrong" discipline as ask.py, minus the repair attempt.

See src/lifecycle_business_tools.py and src/lifecycle_business_agent.py for
why this needed its own tool/grounding stack rather than reusing
src/business_tools.py/src/business_agent.py directly.
"""

from __future__ import annotations

import argparse
import os

from src.lifecycle_answer_models import (
    AnswerValidationError,
    build_unreliable_data_answer,
    business_answer_to_dict,
)
from src.lifecycle_business_agent import LifecycleBusinessAgentError, run_lifecycle_business_qa
from src.lifecycle_business_tools import LifecycleBusinessTools
from src.model_client import DiagnosisModelClient, ModelClientError, OpenAIDiagnosisModelClient
from src.storage import S3Storage

ANSWER_MODEL_ENV_VAR = "ANSWER_MODEL"

METRICS_PIPELINES = ["loan_portfolio", "campaign_funnel", "underwriting_performance", "payment_performance", "delinquency_default"]

IDENTIFYING_COLUMNS = {"breakdown_type", "breakdown_value", "campaign_id", "name", "channel", "as_of_date"}


class AskLifecycleError(Exception):
    """Application-level failure: missing curated artifacts (not a data-reliability issue)."""


def _load_metrics_by_pipeline(storage: S3Storage) -> dict:
    return {pipeline: storage.read_json(f"context/metrics/{pipeline}.json") for pipeline in METRICS_PIPELINES}


def _known_metric_names(business_rules: dict, metrics_by_pipeline: dict) -> set:
    names = set(IDENTIFYING_COLUMNS)
    names.update(business_rules.get("valid_rejection_reasons", []))
    for metrics_doc in metrics_by_pipeline.values():
        names.update(metrics_doc.get("metrics", {}).keys())
    return names


def _load_tools(storage: S3Storage, business_rules: dict, metrics_by_pipeline: dict) -> LifecycleBusinessTools:
    loan_portfolio = storage.read_parquet("curated/loan_portfolio.parquet").iloc[0].to_dict()
    campaign_funnel = storage.read_parquet("curated/campaign_funnel.parquet").to_dict(orient="records")
    underwriting_performance = storage.read_parquet("curated/underwriting_performance.parquet").to_dict(orient="records")
    rejections_df = storage.read_parquet("curated/underwriting_performance_rejections.parquet")
    underwriting_rejections = dict(zip(rejections_df["rejection_reason"], rejections_df["count"].astype(int)))
    payment_performance = storage.read_parquet("curated/payment_performance.parquet").iloc[0].to_dict()
    delinquency_default = storage.read_parquet("curated/delinquency_default.parquet").to_dict(orient="records")

    return LifecycleBusinessTools(
        loan_portfolio=loan_portfolio,
        campaign_funnel=campaign_funnel,
        underwriting_performance=underwriting_performance,
        underwriting_rejections=underwriting_rejections,
        payment_performance=payment_performance,
        delinquency_default=delinquency_default,
        business_rules=business_rules,
        metrics_by_pipeline=metrics_by_pipeline,
    )


def _failed_pipelines(pipeline_run: dict) -> list:
    return [
        name for name, result in pipeline_run.get("pipelines", {}).items()
        if result.get("etl_status") != "SUCCESS" or result.get("validation_status") != "PASS"
    ]


def _attempt_self_heal(storage: S3Storage, model_client_factory) -> dict:
    """Diagnose, repair, and verify the loan_portfolio pipeline. Returns the
    repair_verification dict (or a synthetic one if the flow raised an application-level
    error, e.g. a model/API failure) -- never raises, so the caller can always fall back to
    an UNRELIABLE_DATA answer citing what happened.
    """
    from src.lifecycle_run_self_healing import run_lifecycle_self_healing
    from src.spark_session import get_spark_session

    spark = get_spark_session("lifecycle-self-healing")
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = run_lifecycle_self_healing(spark, storage, model_client_factory, model_client_factory)
        return result["repair_verification"]
    except Exception as exc:  # noqa: BLE001 -- any self-heal failure just means "could not auto-correct"
        return {"verification_status": "NOT_VERIFIED", "summary": f"Automatic repair attempt failed: {exc}"}
    finally:
        spark.stop()


def answer_lifecycle_question(question: str, storage: S3Storage, model_client_factory) -> dict:
    """Answer a business question from the curated lifecycle data. Returns the result dict
    (also written to s3://<bucket>/curated/lifecycle_answer.json).

    If the loan_portfolio pipeline (the one pipeline with diagnose/repair/verify machinery)
    is the cause of a failed pipeline_run, this automatically attempts to diagnose, repair,
    and verify it in an isolated workspace before answering -- returning the CORRECTED
    answer if that repair is fully VERIFIED, and only otherwise falling back to refusing to
    answer. No other pipeline has repair machinery yet, so a failure there still refuses.
    """
    if not storage.exists("curated/pipeline_run.json"):
        raise AskLifecycleError(
            "curated/pipeline_run.json not found -- run python3 -m src.run_lifecycle_etl_pipelines first"
        )
    pipeline_run = storage.read_json("curated/pipeline_run.json")
    self_heal_summary = None

    if pipeline_run.get("overall_status") != "SUCCESS":
        failed = _failed_pipelines(pipeline_run)

        if "loan_portfolio" in failed:
            repair_verification = _attempt_self_heal(storage, model_client_factory)
            self_heal_summary = repair_verification.get("summary")
            if repair_verification.get("verification_status") == "VERIFIED":
                pipeline_run = storage.read_json("curated/pipeline_run.json")
                failed = _failed_pipelines(pipeline_run)

        if pipeline_run.get("overall_status") != "SUCCESS" or failed:
            reason = f"The curated lifecycle data failed validation in these pipelines: {failed or 'unknown'}. Rerun python3 -m src.run_lifecycle_etl_pipelines and investigate before trusting this data."
            if self_heal_summary:
                reason += f" An automatic repair was attempted: {self_heal_summary}"
            answer = build_unreliable_data_answer(question, reason)
            result = {"answer": business_answer_to_dict(answer), "self_heal": self_heal_summary}
            storage.write_json("curated/lifecycle_answer.json", result)
            return result

    business_rules = storage.read_json("context/business_rules.json")
    metrics_by_pipeline = _load_metrics_by_pipeline(storage)
    known_metrics = _known_metric_names(business_rules, metrics_by_pipeline)

    try:
        tools = _load_tools(storage, business_rules, metrics_by_pipeline)
        model_client = model_client_factory()
        answer = run_lifecycle_business_qa(question, tools, model_client, known_metric_names=known_metrics)
    except (LifecycleBusinessAgentError, AnswerValidationError, ModelClientError) as exc:
        answer = build_unreliable_data_answer(question, f"Could not produce a grounded answer: {exc}")

    result = {"answer": business_answer_to_dict(answer), "self_heal": self_heal_summary}
    storage.write_json("curated/lifecycle_answer.json", result)
    return result


def print_result(result: dict) -> None:
    answer = result["answer"]
    if result.get("self_heal"):
        print(f"Self-heal attempted: {result['self_heal']}")
    print("Answer")
    print(f"  status: {answer['answer_status']}")
    print(f"  {answer['answer_summary']}")
    if answer["cited_metrics"]:
        print("  cited metrics:")
        for metric in answer["cited_metrics"]:
            print(f"    {metric['metric_name']} = {metric['value']}")
    if answer["caveats"]:
        print("  caveats:")
        for caveat in answer["caveats"]:
            print(f"    - {caveat}")


def parse_args(argv: list = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Answer a business question from the curated lifecycle pipeline outputs in S3."
    )
    parser.add_argument("question", type=str, help="e.g. 'Which campaign produced the most funded loans?'")
    parser.add_argument("--answer-model", type=str, default=None)
    return parser.parse_args(argv)


def main(argv: list = None) -> None:
    args = parse_args(argv)
    answer_model = args.answer_model or os.environ.get(ANSWER_MODEL_ENV_VAR)

    def _client() -> DiagnosisModelClient:
        return OpenAIDiagnosisModelClient(model=answer_model) if answer_model else OpenAIDiagnosisModelClient()

    try:
        storage = S3Storage()
        result = answer_lifecycle_question(args.question, storage, _client)
    except AskLifecycleError as exc:
        print(f"Could not process the question: {exc}")
        raise SystemExit(1)

    print_result(result)


if __name__ == "__main__":
    main()
