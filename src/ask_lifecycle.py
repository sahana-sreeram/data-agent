"""Answer a business question from the 5 curated lifecycle pipeline outputs in S3.

Unlike the original ask.py's manifest-driven orchestration, this determines "question
lineage" empirically: it answers the question once against current curated data, sees
which tool(s) the model actually called, and only treats a pipeline as relevant if a tool
belonging to it was called. A pipeline failure the question never touched neither blocks
nor triggers a repair attempt -- only a failure in a pipeline the question actually needed
does. When that happens, this automatically diagnoses/repairs/verifies every relevant,
failing pipeline (via src/lifecycle_run_self_healing.py, generalized across all 5
pipelines) and, if every one of them fully verifies, re-answers from the now-corrected
data. Only when a relevant pipeline can't be fixed does this fall back to refusing to
answer, exactly like ask.py's "don't fabricate on top of known-bad data" discipline.

See src/lifecycle_business_tools.py and src/lifecycle_business_agent.py for why this
needed its own tool/grounding stack rather than reusing src/business_tools.py/
src/business_agent.py directly.
"""

from __future__ import annotations

import argparse
import os

from src.context_retriever import ContextRetriever
from src.context_store.file_store import FileContextStore
from src.lifecycle_answer_models import (
    AnswerValidationError,
    build_unreliable_data_answer,
    business_answer_to_dict,
)
from src.lifecycle_business_agent import LifecycleBusinessAgentError, run_lifecycle_business_qa
from src.lifecycle_business_tools import LifecycleBusinessTools
from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY
from src.model_client import (
    DiagnosisModelClient,
    ModelClientError,
    OpenAIDiagnosisModelClient,
    OpenAIResponsesModelClient,
)
from src.storage import S3Storage

ANSWER_MODEL_ENV_VAR = "ANSWER_MODEL"

METRICS_PIPELINES = ["loan_portfolio", "campaign_funnel", "underwriting_performance", "payment_performance", "delinquency_default", "coupon_performance"]

IDENTIFYING_COLUMNS = {"breakdown_type", "breakdown_value", "campaign_id", "name", "channel", "as_of_date"}

# Which pipeline a Q&A tool's result comes from -- the "question lineage" used to decide
# whether a currently-failing pipeline actually matters for THIS question.
# get_metric_definition is deliberately absent: its relevant pipeline comes from its own
# "pipeline" argument instead (see _relevant_pipelines_from). get_business_rules is
# cross-cutting (not backed by any one pipeline's curated/pipeline_run-guarded data) and
# contributes no pipeline.
TOOL_NAME_TO_PIPELINE = {
    "get_loan_portfolio_summary": "loan_portfolio",
    "get_campaign_funnel": "campaign_funnel",
    "get_coupon_performance": "coupon_performance",
    "get_underwriting_performance": "underwriting_performance",
    "get_underwriting_rejection_distribution": "underwriting_performance",
    "get_payment_performance_summary": "payment_performance",
    "get_delinquency_default": "delinquency_default",
}

# The 3 bounded query tools (src/lifecycle_business_tools.py) name which curated table(s)
# they touch via an argument rather than the tool name itself -- without this map, a
# question answered via e.g. aggregate_curated_data(dataset="campaign_funnel", ...) would
# never register campaign_funnel as relevant, silently breaking self-heal triggering and the
# "don't answer from known-bad data" guarantee for any question using these tools.
DATASET_ARG_TOOL_NAMES = {
    "aggregate_curated_data": ("dataset",),
    "sample_curated_data": ("dataset",),
    "join_curated_data": ("left_dataset", "right_dataset"),
}


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
    coupon_performance = storage.read_parquet("curated/coupon_performance.parquet").to_dict(orient="records")

    return LifecycleBusinessTools(
        loan_portfolio=loan_portfolio,
        campaign_funnel=campaign_funnel,
        underwriting_performance=underwriting_performance,
        underwriting_rejections=underwriting_rejections,
        payment_performance=payment_performance,
        delinquency_default=delinquency_default,
        coupon_performance=coupon_performance,
        business_rules=business_rules,
        metrics_by_pipeline=metrics_by_pipeline,
        context_retriever=ContextRetriever(store=FileContextStore()),
        storage=storage,
    )


def _failed_pipelines(pipeline_run: dict) -> set:
    return {
        name for name, result in pipeline_run.get("pipelines", {}).items()
        if result.get("etl_status") != "SUCCESS" or result.get("validation_status") != "PASS"
    }


def _relevant_pipelines_from(called_tool_calls: list) -> set:
    """Which pipeline(s) a question actually needed data from, derived from the tools the
    QA agent actually called this session -- the "question lineage.\""""
    relevant: set = set()
    for call in called_tool_calls:
        name = call["name"]
        arguments = call.get("arguments", {})
        if name == "get_metric_definition":
            pipeline = arguments.get("pipeline")
            if pipeline:
                relevant.add(pipeline)
        elif name in DATASET_ARG_TOOL_NAMES:
            for arg_name in DATASET_ARG_TOOL_NAMES[name]:
                dataset = arguments.get(arg_name)
                if dataset:
                    relevant.add(dataset)
        elif name in TOOL_NAME_TO_PIPELINE:
            relevant.add(TOOL_NAME_TO_PIPELINE[name])
    return relevant


def _repair_model_client_factory() -> DiagnosisModelClient:
    """Repair planning gets its own, dedicated model choice -- a Codex-branded model via the
    Responses API (see src/model_client.py's OpenAIResponsesModelClient) -- independent of
    whatever model diagnosis/Q&A are using. REPAIR_MODEL overrides the default."""
    from src.lifecycle_apply_repair import REPAIR_MODEL_ENV_VAR

    model_name = os.environ.get(REPAIR_MODEL_ENV_VAR)
    return OpenAIResponsesModelClient(model=model_name) if model_name else OpenAIResponsesModelClient()


def _attempt_self_heal(
    pipeline_name: str,
    storage: S3Storage,
    diagnosis_model_client_factory,
    *,
    mode: str = "auto_promote",
    human_approved_categories: frozenset[str] = frozenset(),
    repair_model_client_factory=None,
) -> dict:
    """Diagnose, repair, and verify one lifecycle pipeline. Returns the full
    {run_id, diagnosis, repair_plan, repair_result, repair_verification} dict (or a synthetic
    one with only repair_verification populated if the flow raised an application-level
    error, e.g. a model/API failure) -- never raises, so the caller can always fall back to an
    UNRELIABLE_DATA answer citing what happened, and a UI can always render the same shape
    regardless of how far the flow got.

    mode is passed straight through to run_lifecycle_self_healing (auto_promote/create_pr/
    diagnose_only/propose_patch -- see src/lifecycle_run_self_healing.py); defaulting to
    "auto_promote" preserves this function's original behavior exactly for every existing
    caller. human_approved_categories is likewise passed straight through, empty by default
    (see run_lifecycle_self_healing's docstring for what it's for -- a human explicitly
    approving a candidate repair for a normally-refused category, e.g. SOURCE_CONTRACT_CHANGE,
    only ever as a create_pr candidate).

    repair_model_client_factory defaults to None, which preserves this function's original
    behavior exactly (the module-level _repair_model_client_factory, a real OpenAI Responses
    client) for every existing caller. Passing one overrides which client repair planning
    uses -- e.g. src.demo.enterprise_incident's scripted, no-API-cost repair client -- without
    touching what model diagnosis uses.
    """
    from src.lifecycle_run_self_healing import run_lifecycle_self_healing
    from src.spark_session import get_spark_session

    spark = get_spark_session("lifecycle-self-healing")
    spark.sparkContext.setLogLevel("WARN")
    try:
        return run_lifecycle_self_healing(
            pipeline_name,
            spark,
            storage,
            diagnosis_model_client_factory,
            repair_model_client_factory or _repair_model_client_factory,
            mode=mode,
            human_approved_categories=human_approved_categories,
        )
    except Exception as exc:  # noqa: BLE001 -- any self-heal failure just means "could not auto-correct"
        return {
            "run_id": None,
            "diagnosis": None,
            "repair_plan": None,
            "repair_result": None,
            "repair_verification": {"verification_status": "NOT_VERIFIED", "summary": f"Automatic repair attempt failed: {exc}"},
        }
    finally:
        spark.stop()


def _answer_once(question: str, storage: S3Storage, model_client_factory):
    """Run the QA loop once against current curated data. Returns (answer_dict_or_None,
    LifecycleQAResult_or_None, error_answer_or_None) -- exactly one of the last two is set."""
    business_rules = storage.read_json("context/business_rules.json")
    metrics_by_pipeline = _load_metrics_by_pipeline(storage)
    known_metrics = _known_metric_names(business_rules, metrics_by_pipeline)

    try:
        tools = _load_tools(storage, business_rules, metrics_by_pipeline)
        model_client = model_client_factory()
        qa_result = run_lifecycle_business_qa(question, tools, model_client, known_metric_names=known_metrics)
        return qa_result, None
    except (LifecycleBusinessAgentError, AnswerValidationError, ModelClientError) as exc:
        return None, build_unreliable_data_answer(question, f"Could not produce a grounded answer: {exc}")


def answer_from_candidate(
    question: str,
    storage: S3Storage,
    model_client_factory,
    pipeline_name: str,
    candidate_metrics_after: dict,
) -> dict:
    """Answer a question using one pipeline's CANDIDATE curated output -- e.g.
    repair_verification["metrics_after"] from a mode="create_pr" self-healing run -- instead
    of that pipeline's real, not-yet-promoted S3 data. Every other pipeline's data still comes
    from real storage, unchanged. This is how a create_pr run can show what the corrected
    answer WOULD be without the candidate ever being written anywhere real.

    Only substitutes single-row pipelines (loan_portfolio, payment_performance) correctly as
    written -- the only ones this vertical slice's repair flow targets; a multi-row
    pipeline's LifecycleBusinessTools._registry would also need rebuilding to reflect a
    substituted candidate, which is out of scope until a multi-row pipeline is migrated.
    """
    business_rules = storage.read_json("context/business_rules.json")
    metrics_by_pipeline = _load_metrics_by_pipeline(storage)
    tools = _load_tools(storage, business_rules, metrics_by_pipeline)

    curated_key = PIPELINE_REGISTRY[pipeline_name].curated_keys[0]
    records = candidate_metrics_after.get(curated_key)
    if records:
        setattr(tools, pipeline_name, records[0] if len(records) == 1 else records)

    known_metrics = _known_metric_names(business_rules, metrics_by_pipeline)
    model_client = model_client_factory()
    qa_result = run_lifecycle_business_qa(question, tools, model_client, known_metric_names=known_metrics)
    return business_answer_to_dict(qa_result.answer)


def _validation_failures_from(self_heal: dict) -> dict:
    return {
        pipeline_name: heal["repair_verification"].get("failed_checks_before", [])
        for pipeline_name, heal in self_heal.items()
    }


def answer_lifecycle_question(question: str, storage: S3Storage, model_client_factory, *, mode: str = "auto_promote") -> dict:
    """Answer a business question from the curated lifecycle data. Returns the result dict
    (also written to s3://<bucket>/curated/lifecycle_answer.json):

    {"question", "relevant_pipelines", "validation_failures", "answer", "self_heal",
    "corrected_answer"} -- self_heal (when not None) maps each broken, relevant pipeline to
    its full {run_id, diagnosis, repair_plan, repair_result, repair_verification} self-healing
    attempt; corrected_answer is set only when every one of them fully verified AND was
    actually promoted (never true for mode="create_pr", which by design never promotes --
    see src.data_ops.run_incident_response for how a create_pr run's CANDIDATE answer is
    shown instead, via answer_from_candidate, without conflating it with a real, promoted
    correction).

    mode is passed straight through to each self-heal attempt (default "auto_promote"
    preserves this function's original behavior exactly for every existing caller).
    """
    if not storage.exists("curated/pipeline_run.json"):
        raise AskLifecycleError(
            "curated/pipeline_run.json not found -- run python3 -m src.run_lifecycle_etl_pipelines first"
        )
    pipeline_run = storage.read_json("curated/pipeline_run.json")
    failed = _failed_pipelines(pipeline_run)

    qa_result, error_answer = _answer_once(question, storage, model_client_factory)
    if error_answer is not None:
        result = {
            "question": question,
            "relevant_pipelines": [],
            "validation_failures": {},
            "answer": business_answer_to_dict(error_answer),
            "self_heal": None,
            "corrected_answer": None,
        }
        storage.write_json("curated/lifecycle_answer.json", result)
        return result

    relevant = _relevant_pipelines_from(qa_result.called_tool_calls)
    broken_relevant = relevant & failed

    if not broken_relevant:
        # Either fully healthy, or the question only touched healthy pipelines -- the
        # answer we already have is trustworthy regardless of any UNRELATED failure.
        result = {
            "question": question,
            "relevant_pipelines": sorted(relevant),
            "validation_failures": {},
            "answer": business_answer_to_dict(qa_result.answer),
            "self_heal": None,
            "corrected_answer": None,
        }
        storage.write_json("curated/lifecycle_answer.json", result)
        return result

    # The first answer was grounded in at least one broken, relevant pipeline's data --
    # discard it (never return an answer built on known-bad data) and try to fix each one.
    self_heal: dict = {}
    for pipeline_name in sorted(broken_relevant):
        self_heal[pipeline_name] = _attempt_self_heal(pipeline_name, storage, model_client_factory, mode=mode)
    validation_failures = _validation_failures_from(self_heal)

    still_broken = broken_relevant & _failed_pipelines(storage.read_json("curated/pipeline_run.json"))

    if not still_broken:
        # Every relevant, previously-broken pipeline is now verified healthy -- re-answer
        # fresh from the corrected data rather than returning the stale first answer.
        qa_result2, error_answer2 = _answer_once(question, storage, model_client_factory)
        corrected_answer = business_answer_to_dict(error_answer2 if error_answer2 is not None else qa_result2.answer)
        result = {
            "question": question,
            "relevant_pipelines": sorted(relevant),
            "validation_failures": validation_failures,
            "answer": business_answer_to_dict(qa_result.answer),
            "self_heal": self_heal,
            "corrected_answer": corrected_answer,
        }
        storage.write_json("curated/lifecycle_answer.json", result)
        return result

    reason = (
        f"The curated lifecycle data this question depends on failed validation in: "
        f"{sorted(still_broken)}. Rerun python3 -m src.run_lifecycle_etl_pipelines and investigate before trusting this data."
    )
    for pipeline_name, heal in self_heal.items():
        reason += f" [{pipeline_name}] automatic repair attempt: {heal['repair_verification'].get('summary')}"
    answer = build_unreliable_data_answer(question, reason)
    result = {
        "question": question,
        "relevant_pipelines": sorted(relevant),
        "validation_failures": validation_failures,
        "answer": business_answer_to_dict(answer),
        "self_heal": self_heal,
        "corrected_answer": None,
    }
    storage.write_json("curated/lifecycle_answer.json", result)
    return result


def print_result(result: dict) -> None:
    print(f"Question: {result['question']}")
    if result.get("relevant_pipelines"):
        print(f"Relevant pipelines: {', '.join(result['relevant_pipelines'])}")
    if result.get("self_heal"):
        for pipeline_name, heal in result["self_heal"].items():
            verification = heal["repair_verification"]
            print(f"Self-heal attempted [{pipeline_name}]: {verification.get('summary')}")
            if heal.get("diagnosis"):
                diagnosis = heal["diagnosis"]
                print(f"  diagnosis: {diagnosis.get('root_cause_category')} -- {diagnosis.get('root_cause')}")
            if heal.get("repair_result"):
                print(f"  repair_status: {heal['repair_result'].get('repair_status')}")
            print(f"  verification_status: {verification.get('verification_status')}")

    def _print_answer(label: str, answer: dict) -> None:
        print(label)
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

    _print_answer("Answer", result["answer"])
    if result.get("corrected_answer"):
        _print_answer("Corrected answer", result["corrected_answer"])


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
