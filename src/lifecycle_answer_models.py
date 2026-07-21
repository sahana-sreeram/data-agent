"""Generalized grounding for the lifecycle Q&A agent's answers.

Reuses BusinessAnswer/AnswerStatus/CitedMetric/AnswerValidationError/
build_unreliable_data_answer/business_answer_to_dict, AND the generic
answer-shape validation helpers (_require_keys/_parse_enum/
_require_non_empty_string/_require_string_list), from src/answer_models.py
completely unchanged -- those are already generic, not portfolio_summary-specific.

Only the grounding check itself needs a new implementation:
src/answer_models.py's parse_business_answer() grounds against a single fixed
dict (`portfolio_summary.get(metric_name) == value`). The lifecycle tools
return EITHER a single flat dict OR a list of rows (`{"rows": [...]}`) --
grounding here checks that a cited (metric_name, value) pair appears
SOMEWHERE in a result actually returned by the cited tool this session,
searching across every call made to that tool.

Known, deliberate simplification (documented, not accidental): this does not
verify the model attributed a value to the *correct* row when a tool returns
multiple rows (e.g. citing campaign A's loans_funded while discussing
campaign B) -- only that the number is real and came from that tool. Closing
that fully would require a required row-identifying field on every citation
and is left as a follow-up, not built here.
"""

from __future__ import annotations

from src.answer_models import (
    AnswerStatus,
    AnswerValidationError,
    BusinessAnswer,
    CitedMetric,
    _require_keys,
    _require_non_empty_string,
    _require_string_list,
    _parse_enum,
    build_unreliable_data_answer,
    business_answer_to_dict,
)

__all__ = [
    "AnswerStatus",
    "AnswerValidationError",
    "BusinessAnswer",
    "CitedMetric",
    "build_unreliable_data_answer",
    "business_answer_to_dict",
    "parse_lifecycle_business_answer",
]

_REQUIRED_KEYS = ("answer_status", "question", "answer_summary", "as_of_date", "cited_metrics", "caveats")
_REQUIRED_METRIC_KEYS = ("metric_name", "value", "source_reference")


def _result_contains_metric(result, metric_name: str, value) -> bool:
    """Search a single tool result (a flat dict, or a {"rows": [...]} wrapper) for a
    matching (metric_name, value) pair.
    """
    if not isinstance(result, dict):
        return False
    rows = result.get("rows")
    if isinstance(rows, list):
        return any(isinstance(row, dict) and row.get(metric_name) == value for row in rows)
    return metric_name in result and result[metric_name] == value


def _parse_cited_metric(raw, *, called_tool_names: set, known_metric_names: set, tool_results_by_name: dict) -> CitedMetric:
    _require_keys(raw, _REQUIRED_METRIC_KEYS, "cited_metric")
    metric_name = raw["metric_name"]
    if metric_name not in known_metric_names:
        raise AnswerValidationError(f"cited_metrics contains an unknown metric name: {metric_name!r}; known: {sorted(known_metric_names)}")

    source_reference = raw["source_reference"]
    if not isinstance(source_reference, str):
        raise AnswerValidationError(f"cited_metric.source_reference must be a string, got {source_reference!r}")
    normalized_reference = source_reference.removeprefix("functions.")
    if normalized_reference not in called_tool_names:
        raise AnswerValidationError(
            f"cited_metric.source_reference {source_reference!r} does not match a tool actually called this session"
        )

    value = raw["value"]
    results = tool_results_by_name.get(normalized_reference, [])
    if not any(_result_contains_metric(result, metric_name, value) for result in results):
        raise AnswerValidationError(
            f"cited_metric value for {metric_name!r} ({value!r}) was not found in any result returned by "
            f"{normalized_reference!r} this session -- the agent must report exactly what a tool returned, "
            "never a paraphrased, rounded, or fabricated number"
        )

    return CitedMetric(metric_name=metric_name, value=value, source_reference=normalized_reference)


def parse_lifecycle_business_answer(
    raw: dict,
    *,
    called_tool_names: set,
    known_metric_names: set,
    tool_results_by_name: dict,
) -> BusinessAnswer:
    """Validate and parse a raw model-submitted answer dict against the lifecycle tool results.

    Raises AnswerValidationError for any structural, enum, or grounding violation.
    """
    _require_keys(raw, _REQUIRED_KEYS, "answer")

    answer_status = _parse_enum(AnswerStatus, raw["answer_status"], "answer_status")
    question = _require_non_empty_string(raw["question"], "question")
    answer_summary = _require_non_empty_string(raw["answer_summary"], "answer_summary")
    caveats = _require_string_list(raw["caveats"], "caveats")

    as_of_date = raw["as_of_date"]
    if as_of_date is not None and not isinstance(as_of_date, str):
        raise AnswerValidationError("as_of_date must be a string or null")

    cited_raw = raw["cited_metrics"]
    if not isinstance(cited_raw, list):
        raise AnswerValidationError("cited_metrics must be a list")
    cited_metrics = [
        _parse_cited_metric(
            item, called_tool_names=called_tool_names, known_metric_names=known_metric_names, tool_results_by_name=tool_results_by_name
        )
        for item in cited_raw
    ]

    if answer_status == AnswerStatus.ANSWERED and not cited_metrics:
        raise AnswerValidationError("answer_status=ANSWERED requires at least one cited metric")

    if answer_status in (AnswerStatus.UNRELIABLE_DATA, AnswerStatus.INSUFFICIENT_DATA) and not caveats:
        raise AnswerValidationError(f"answer_status={answer_status.value} requires at least one caveat explaining why")

    return BusinessAnswer(
        answer_status=answer_status,
        question=question,
        answer_summary=answer_summary,
        as_of_date=as_of_date,
        cited_metrics=cited_metrics,
        caveats=caveats,
    )
