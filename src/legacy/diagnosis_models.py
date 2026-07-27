"""Structured, validated diagnosis output for the diagnosis agent.

Defines the DiagnosisResult schema and parse_diagnosis_result(), which
enforces every grounding and structural requirement the milestone requires:
valid enums, evidence that traces to a real tool call or known file, and
status-specific requirements (DIAGNOSED needs evidence, NO_INCIDENT needs a
passing validation, INSUFFICIENT_EVIDENCE needs a stated gap). Malformed
model output is rejected here -- never silently coerced into something
that looks like a valid diagnosis.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum


class DiagnosisStatus(str, Enum):
    DIAGNOSED = "DIAGNOSED"
    NO_INCIDENT = "NO_INCIDENT"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


class RootCauseCategory(str, Enum):
    SOURCE_CONTRACT_CHANGE = "SOURCE_CONTRACT_CHANGE"
    ETL_LOGIC = "ETL_LOGIC"
    MISSING_DATA = "MISSING_DATA"
    DUPLICATION = "DUPLICATION"
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"
    BUSINESS_RULE_MISMATCH = "BUSINESS_RULE_MISMATCH"
    UNKNOWN = "UNKNOWN"


class EvidenceSourceType(str, Enum):
    VALIDATION = "VALIDATION"
    BUSINESS_RULE = "BUSINESS_RULE"
    RAW_DATA = "RAW_DATA"
    LINEAGE = "LINEAGE"
    ETL_SOURCE = "ETL_SOURCE"
    PIPELINE_METADATA = "PIPELINE_METADATA"


class Confidence(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class FixScope(str, Enum):
    MINIMAL = "MINIMAL"
    BROADER_REVIEW = "BROADER_REVIEW"


class DiagnosisValidationError(Exception):
    """Raised when model output does not conform to the diagnosis schema or grounding rules."""


@dataclass(frozen=True)
class Evidence:
    source_type: EvidenceSourceType
    source_reference: str
    finding: str
    expected: str | None
    actual: str | None


@dataclass(frozen=True)
class RecommendedFix:
    target_file: str | None
    change_summary: str
    scope: FixScope


@dataclass(frozen=True)
class DiagnosisResult:
    diagnosis_status: DiagnosisStatus
    incident_summary: str
    affected_metrics: list[str]
    root_cause_category: RootCauseCategory
    # initiating_event: the external trigger, if any, distinct from what needs
    # fixing -- e.g. an approved upstream/business-rule change. Null when
    # there's no separate trigger (e.g. a plain code bug with no external
    # cause). root_cause is always the REPAIRABLE cause: the specific thing
    # that must change to fix the incident, even when an initiating_event
    # also exists. The two must not be conflated -- a valid initiating event
    # (like an approved contract change) does not, by itself, explain why a
    # downstream component failed to keep up with it.
    initiating_event: str | None
    root_cause: str
    reasoning_summary: str
    evidence: list[Evidence]
    recommended_fix: RecommendedFix | None
    confidence: Confidence
    uncertainties: list[str]
    additional_evidence_needed: list[str]


_REQUIRED_KEYS = (
    "diagnosis_status", "incident_summary", "affected_metrics", "root_cause_category",
    "initiating_event", "root_cause", "reasoning_summary", "evidence", "recommended_fix",
    "confidence", "uncertainties", "additional_evidence_needed",
)
_REQUIRED_EVIDENCE_KEYS = ("source_type", "source_reference", "finding", "expected", "actual")
_REQUIRED_FIX_KEYS = ("target_file", "change_summary", "scope")
_NON_EMPTY_STRING_FIELDS = ("incident_summary", "root_cause", "reasoning_summary")


def _require_keys(raw, keys: tuple, label: str) -> None:
    if not isinstance(raw, dict):
        raise DiagnosisValidationError(f"{label} must be a JSON object, got {type(raw).__name__}")
    missing = [k for k in keys if k not in raw]
    if missing:
        raise DiagnosisValidationError(f"{label} is missing required keys: {missing}")


def _parse_enum(enum_cls, value, field_name: str):
    try:
        return enum_cls(value)
    except ValueError:
        valid = [e.value for e in enum_cls]
        raise DiagnosisValidationError(f"invalid {field_name} '{value}'; must be one of {valid}")


def _require_string_list(value, field_name: str) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise DiagnosisValidationError(f"{field_name} must be a list of strings")
    return value


def _require_non_empty_string(value, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DiagnosisValidationError(f"{field_name} must be a non-empty string")
    return value


def _require_nullable_non_empty_string(value, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_non_empty_string(value, field_name)


def _parse_evidence_item(raw, allowed_sources: set[str]) -> Evidence:
    _require_keys(raw, _REQUIRED_EVIDENCE_KEYS, "evidence item")
    source_type = _parse_enum(EvidenceSourceType, raw["source_type"], "evidence.source_type")
    source_reference = raw["source_reference"]
    if not isinstance(source_reference, str):
        raise DiagnosisValidationError(f"evidence.source_reference must be a string, got {source_reference!r}")
    # Models sometimes echo tool-call names back with a "functions." namespace
    # prefix (an OpenAI tool-calling convention) even though the actual
    # dispatched tool name has no prefix. Normalize before grounding-checking
    # so this cosmetic variant isn't mistaken for an ungrounded/invented
    # source -- we still reject anything that doesn't resolve to a real,
    # actually-called tool or known file path.
    normalized_reference = source_reference.removeprefix("functions.")
    if normalized_reference not in allowed_sources:
        raise DiagnosisValidationError(
            f"evidence.source_reference {source_reference!r} does not match any tool actually called "
            f"or known file path this session; allowed: {sorted(allowed_sources)}"
        )
    finding = _require_non_empty_string(raw["finding"], "evidence.finding")
    return Evidence(
        source_type=source_type,
        source_reference=normalized_reference,
        finding=finding,
        expected=raw.get("expected"),
        actual=raw.get("actual"),
    )


def _parse_recommended_fix(raw, known_file_paths: set[str]) -> RecommendedFix | None:
    if raw is None:
        return None
    _require_keys(raw, _REQUIRED_FIX_KEYS, "recommended_fix")
    target_file = raw["target_file"]
    if target_file is not None and target_file not in known_file_paths:
        raise DiagnosisValidationError(
            f"recommended_fix.target_file {target_file!r} is not a known repository file; "
            f"known files: {sorted(known_file_paths)}"
        )
    scope = _parse_enum(FixScope, raw["scope"], "recommended_fix.scope")
    change_summary = _require_non_empty_string(raw["change_summary"], "recommended_fix.change_summary")
    return RecommendedFix(target_file=target_file, change_summary=change_summary, scope=scope)


def parse_diagnosis_result(
    raw,
    *,
    validation_overall_status: str,
    called_tool_names: set[str],
    known_metric_names: set[str],
    known_file_paths: set[str],
) -> DiagnosisResult:
    """Validate and parse a raw model-submitted diagnosis dict.

    Raises DiagnosisValidationError for any structural, enum, or grounding
    violation. Callers must not fall back to a partially-trusted result --
    a rejected diagnosis is an application-level failure, not a diagnosis.
    """
    _require_keys(raw, _REQUIRED_KEYS, "diagnosis")

    diagnosis_status = _parse_enum(DiagnosisStatus, raw["diagnosis_status"], "diagnosis_status")

    if diagnosis_status == DiagnosisStatus.NO_INCIDENT and validation_overall_status != "PASS":
        raise DiagnosisValidationError(
            "diagnosis_status=NO_INCIDENT is only valid when validation overall_status is PASS "
            f"(actual validation overall_status: {validation_overall_status!r})"
        )

    affected_metrics = _require_string_list(raw["affected_metrics"], "affected_metrics")
    unknown_metrics = [m for m in affected_metrics if m not in known_metric_names]
    if unknown_metrics:
        raise DiagnosisValidationError(
            f"affected_metrics contains unknown metric names: {unknown_metrics}; "
            f"known metrics: {sorted(known_metric_names)}"
        )

    root_cause_category = _parse_enum(RootCauseCategory, raw["root_cause_category"], "root_cause_category")
    confidence = _parse_enum(Confidence, raw["confidence"], "confidence")

    allowed_sources = set(called_tool_names) | set(known_file_paths)
    evidence_raw = raw["evidence"]
    if not isinstance(evidence_raw, list):
        raise DiagnosisValidationError("evidence must be a list")
    evidence = [_parse_evidence_item(item, allowed_sources) for item in evidence_raw]

    if diagnosis_status == DiagnosisStatus.DIAGNOSED and not evidence:
        raise DiagnosisValidationError("diagnosis_status=DIAGNOSED requires at least one evidence item")

    additional_evidence_needed = _require_string_list(
        raw["additional_evidence_needed"], "additional_evidence_needed"
    )
    if diagnosis_status == DiagnosisStatus.INSUFFICIENT_EVIDENCE and not additional_evidence_needed:
        raise DiagnosisValidationError(
            "diagnosis_status=INSUFFICIENT_EVIDENCE requires at least one entry in additional_evidence_needed"
        )

    uncertainties = _require_string_list(raw["uncertainties"], "uncertainties")
    recommended_fix = _parse_recommended_fix(raw["recommended_fix"], known_file_paths)
    initiating_event = _require_nullable_non_empty_string(raw["initiating_event"], "initiating_event")

    for field_name in _NON_EMPTY_STRING_FIELDS:
        _require_non_empty_string(raw[field_name], field_name)

    return DiagnosisResult(
        diagnosis_status=diagnosis_status,
        incident_summary=raw["incident_summary"],
        affected_metrics=affected_metrics,
        root_cause_category=root_cause_category,
        initiating_event=initiating_event,
        root_cause=raw["root_cause"],
        reasoning_summary=raw["reasoning_summary"],
        evidence=evidence,
        recommended_fix=recommended_fix,
        confidence=confidence,
        uncertainties=uncertainties,
        additional_evidence_needed=additional_evidence_needed,
    )


def build_no_incident_diagnosis() -> DiagnosisResult:
    """The fixed NO_INCIDENT result for a clean (PASS) validation run -- no model call needed."""
    return DiagnosisResult(
        diagnosis_status=DiagnosisStatus.NO_INCIDENT,
        incident_summary="Validation passed; no incident requires diagnosis.",
        affected_metrics=[],
        root_cause_category=RootCauseCategory.UNKNOWN,
        initiating_event=None,
        root_cause="Not applicable -- validation passed.",
        reasoning_summary="Validation passed; no incident requires diagnosis.",
        evidence=[],
        recommended_fix=None,
        confidence=Confidence.HIGH,
        uncertainties=[],
        additional_evidence_needed=[],
    )


def diagnosis_to_dict(result: DiagnosisResult) -> dict:
    """Serialize a DiagnosisResult to a JSON-compatible dict (enums -> their .value)."""
    d = asdict(result)
    d["diagnosis_status"] = result.diagnosis_status.value
    d["root_cause_category"] = result.root_cause_category.value
    d["confidence"] = result.confidence.value
    d["evidence"] = [{**asdict(item), "source_type": item.source_type.value} for item in result.evidence]
    if result.recommended_fix is not None:
        d["recommended_fix"] = {**asdict(result.recommended_fix), "scope": result.recommended_fix.scope.value}
    else:
        d["recommended_fix"] = None
    return d
