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

A cited_metric now also carries a required `row_identifier` field: for a multi-row tool
result (more than one row returned), it must be a non-empty {field: value} dict pinpointing
which row the citation came from (e.g. {"campaign_id": "CMP0042"} or {"breakdown_type":
"risk_segment", "breakdown_value": "PRIME"}), checked against the row itself before the
metric/value match -- closing the previously-documented gap where a model could cite campaign
A's loans_funded while discussing campaign B, as long as the number happened to appear
somewhere in the same tool's results. row_identifier is optional (may be null) when the cited
tool result has zero or one row, since there's no ambiguity to resolve. "Multi-row" is
detected generically via _rows_from_result, which recognizes every current and new tool
result shape ("rows": [...] from the get_* tools, "groups": [...] from aggregate_dataset,
"samples": [...]/"rows": [...] from sample_dataset/join_datasets).
"""

from __future__ import annotations

from src.legacy.answer_models import (
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
_REQUIRED_METRIC_KEYS = ("metric_name", "value", "source_reference", "row_identifier")


def _rows_from_result(result: dict) -> list | None:
    """Every current and new tool result shape that carries more than one row wraps its
    rows in a list under one of these keys -- get_* tools use "rows", aggregate_dataset uses
    "groups", sample_dataset/join_datasets use "rows"/"samples" respectively. Returns None
    for a flat, single-shape result (no ambiguity to resolve)."""
    for key in ("rows", "groups", "samples"):
        value = result.get(key)
        if isinstance(value, list):
            return value
    return None


def _row_matches_identifier(row: dict, row_identifier: dict) -> bool:
    return all(row.get(k) == v for k, v in row_identifier.items())


def _result_contains_metric(result, metric_name: str, value, row_identifier: dict | None) -> bool:
    """Search a single tool result for a matching (metric_name, value) pair. For a
    multi-row result, row_identifier must narrow the search to the specific row(s) it
    identifies -- an ambiguous (>1 row) result with no identifier (or one matching zero
    rows) never grounds, even if the value happens to appear in some other row.
    """
    if not isinstance(result, dict):
        return False
    rows = _rows_from_result(result)
    if rows is not None:
        if len(rows) > 1:
            if not row_identifier:
                return False
            rows = [row for row in rows if isinstance(row, dict) and _row_matches_identifier(row, row_identifier)]
        return any(isinstance(row, dict) and row.get(metric_name) == value for row in rows)
    return metric_name in result and result[metric_name] == value


def _field_appears_in_results(results: list, metric_name: str) -> bool:
    """Whether metric_name is a real key that actually appeared in some result returned by
    the cited tool this session -- used to accept dynamically-computed field names (e.g.
    aggregate_curated_data's "sum_loans_funded") that can never be exhaustively
    pre-registered in known_metric_names, without weakening the value-grounding check."""
    for result in results:
        if not isinstance(result, dict):
            continue
        rows = _rows_from_result(result)
        if rows is not None:
            if any(isinstance(row, dict) and metric_name in row for row in rows):
                return True
        elif metric_name in result:
            return True
    return False


def _parse_cited_metric(raw, *, called_tool_names: set, known_metric_names: set, tool_results_by_name: dict) -> CitedMetric:
    _require_keys(raw, _REQUIRED_METRIC_KEYS, "cited_metric")
    metric_name = raw["metric_name"]

    source_reference = raw["source_reference"]
    if not isinstance(source_reference, str):
        raise AnswerValidationError(f"cited_metric.source_reference must be a string, got {source_reference!r}")
    normalized_reference = source_reference.removeprefix("functions.")
    if normalized_reference not in called_tool_names and "(" in normalized_reference:
        # Confirmed live: a model sometimes cites a bounded query tool (one that takes
        # arguments, e.g. sample_curated_data) as "tool_name(dataset)" instead of the bare
        # tool name -- a reasonable, self-documenting convention, just not the exact string
        # this session actually called. Strip a trailing "(...)" before matching, same spirit
        # as the "functions." prefix normalization above.
        normalized_reference = normalized_reference.split("(", 1)[0]
    if normalized_reference not in called_tool_names:
        raise AnswerValidationError(
            f"cited_metric.source_reference {source_reference!r} does not match a tool actually called this session"
        )

    row_identifier = raw["row_identifier"]
    if row_identifier is not None and not isinstance(row_identifier, dict):
        raise AnswerValidationError(f"cited_metric.row_identifier must be an object or null, got {row_identifier!r}")

    results = tool_results_by_name.get(normalized_reference, [])
    if metric_name not in known_metric_names and not _field_appears_in_results(results, metric_name):
        raise AnswerValidationError(f"cited_metrics contains an unknown metric name: {metric_name!r}; known: {sorted(known_metric_names)}")

    value = raw["value"]
    if not any(_result_contains_metric(result, metric_name, value, row_identifier) for result in results):
        raise AnswerValidationError(
            f"cited_metric value for {metric_name!r} ({value!r}) with row_identifier {row_identifier!r} was not found in "
            f"any result returned by {normalized_reference!r} this session -- the agent must report exactly what a tool "
            "returned, never a paraphrased, rounded, or fabricated number, and must identify which row a multi-row "
            "result's value came from"
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
