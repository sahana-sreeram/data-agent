"""Structured, validated answer output for the business Q&A agent.

Mirrors diagnosis_models.py/repair_models.py: enums, a frozen dataclass
schema, and parse_business_answer(), which rejects malformed or ungrounded
model output rather than coercing it. The model may choose WHICH metrics
answer a question and how to phrase the summary, but every cited numeric
value must exactly match what the trusted portfolio_summary.json actually
contains -- there is no way for the agent to report a fabricated number
without a grounding check catching it.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum


class AnswerStatus(str, Enum):
    ANSWERED = "ANSWERED"
    UNRELIABLE_DATA = "UNRELIABLE_DATA"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


class AnswerValidationError(Exception):
    """Raised when model output does not conform to the answer schema or grounding rules."""


@dataclass(frozen=True)
class CitedMetric:
    metric_name: str
    value: object
    source_reference: str


@dataclass(frozen=True)
class BusinessAnswer:
    answer_status: AnswerStatus
    question: str
    answer_summary: str
    as_of_date: str | None
    cited_metrics: list
    caveats: list


_REQUIRED_KEYS = ("answer_status", "question", "answer_summary", "as_of_date", "cited_metrics", "caveats")
_REQUIRED_METRIC_KEYS = ("metric_name", "value", "source_reference")


def _require_keys(raw, keys: tuple, label: str) -> None:
    if not isinstance(raw, dict):
        raise AnswerValidationError(f"{label} must be a JSON object, got {type(raw).__name__}")
    missing = [k for k in keys if k not in raw]
    if missing:
        raise AnswerValidationError(f"{label} is missing required keys: {missing}")


def _parse_enum(enum_cls, value, field_name: str):
    try:
        return enum_cls(value)
    except ValueError:
        valid = [e.value for e in enum_cls]
        raise AnswerValidationError(f"invalid {field_name} {value!r}; must be one of {valid}")


def _require_non_empty_string(value, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AnswerValidationError(f"{field_name} must be a non-empty string")
    return value


def _require_string_list(value, field_name: str) -> list:
    if not isinstance(value, list) or not all(isinstance(x, str) for x in value):
        raise AnswerValidationError(f"{field_name} must be a list of strings")
    return value


def _parse_cited_metric(raw, *, called_tool_names: set, known_metric_names: set, portfolio_summary: dict) -> CitedMetric:
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
    actual_value = portfolio_summary.get(metric_name)
    if actual_value != value:
        raise AnswerValidationError(
            f"cited_metric value for {metric_name!r} ({value!r}) does not match the trusted value ({actual_value!r}) -- "
            "the agent must report exactly what the tool returned, never a paraphrased or rounded number"
        )

    return CitedMetric(metric_name=metric_name, value=value, source_reference=normalized_reference)


def parse_business_answer(
    raw: dict,
    *,
    called_tool_names: set,
    known_metric_names: set,
    portfolio_summary: dict,
) -> BusinessAnswer:
    """Validate and parse a raw model-submitted answer dict.

    Raises AnswerValidationError for any structural, enum, or grounding
    violation. Callers must not fall back to a partially-trusted answer.
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
        _parse_cited_metric(item, called_tool_names=called_tool_names, known_metric_names=known_metric_names, portfolio_summary=portfolio_summary)
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


def build_unreliable_data_answer(question: str, reason: str) -> BusinessAnswer:
    """Deterministic UNRELIABLE_DATA answer for when validation fails and could not be auto-repaired -- no model call needed."""
    return BusinessAnswer(
        answer_status=AnswerStatus.UNRELIABLE_DATA,
        question=question,
        answer_summary=(
            "I can't give a reliable number right now -- the underlying data failed validation "
            "and could not be automatically repaired."
        ),
        as_of_date=None,
        cited_metrics=[],
        caveats=[reason],
    )


def business_answer_to_dict(answer: BusinessAnswer) -> dict:
    """Serialize a BusinessAnswer to a JSON-compatible dict (enums -> their .value)."""
    d = asdict(answer)
    d["answer_status"] = answer.answer_status.value
    return d
