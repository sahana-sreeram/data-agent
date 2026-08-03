"""Structured, validated repair-plan output for the repair agent, plus the
deterministic (non-LLM) repair-eligibility gate.

Mirrors diagnosis_models.py's shape deliberately: enums, a frozen dataclass
schema, a parse_*() function that rejects malformed/ungrounded model output
rather than coercing it, and small deterministic constructors for the
no-model-call short-circuit cases.

Two independent responsibilities live here:
  1. evaluate_repair_eligibility() -- pure Python, no LLM. Decides whether an
     incident is even allowed to reach the repair model at all.
  2. parse_repair_plan() -- schema + grounding validation for whatever the
     repair model proposes, once it *is* allowed to run.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum


class RepairEligibility(str, Enum):
    ELIGIBLE_FOR_REPAIR = "ELIGIBLE_FOR_REPAIR"
    HUMAN_REVIEW_REQUIRED = "HUMAN_REVIEW_REQUIRED"
    NO_REPAIR_NEEDED = "NO_REPAIR_NEEDED"
    INVALID_DIAGNOSIS = "INVALID_DIAGNOSIS"


class RepairDecision(str, Enum):
    PROPOSE_REPAIR = "PROPOSE_REPAIR"
    NO_SAFE_REPAIR = "NO_SAFE_REPAIR"
    HUMAN_REVIEW_REQUIRED = "HUMAN_REVIEW_REQUIRED"


class RepairType(str, Enum):
    CONFIGURATION_CHANGE = "CONFIGURATION_CHANGE"
    CODE_CHANGE = "CODE_CHANGE"
    NONE = "NONE"


class PatchFormat(str, Enum):
    UNIFIED_DIFF = "UNIFIED_DIFF"
    STRUCTURED_CONFIG_EDIT = "STRUCTURED_CONFIG_EDIT"
    NONE = "NONE"


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class RepairPlanValidationError(Exception):
    """Raised when model output does not conform to the repair-plan schema or grounding rules."""


# --- Eligibility gate (deterministic, no LLM) ------------------------------

_CONFIDENCE_RANK = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}

# Root-cause categories a diagnosis must have to even be considered for
# automated repair. SOURCE_CONTRACT_CHANGE is deliberately excluded: by
# definition it means the approved rules haven't caught up with an upstream
# change yet, so business semantics are NOT settled -- that always needs a
# human, regardless of confidence. UNKNOWN, MISSING_DATA, SCHEMA_MISMATCH are
# excluded for the same reason (unclear or unbounded scope).
DEFAULT_ELIGIBLE_ROOT_CAUSE_CATEGORIES = frozenset({"BUSINESS_RULE_MISMATCH", "ETL_LOGIC", "DUPLICATION"})
DEFAULT_CONFIDENCE_THRESHOLD = "HIGH"

_REQUIRED_DIAGNOSIS_KEYS = (
    "diagnosis_status", "root_cause_category", "confidence", "recommended_fix", "evidence",
)


@dataclass(frozen=True)
class EligibilityDecision:
    decision: RepairEligibility
    reasons: list[str]


def evaluate_repair_eligibility(
    diagnosis: dict,
    *,
    allowed_target_files: set[str],
    eligible_root_cause_categories: frozenset[str] = DEFAULT_ELIGIBLE_ROOT_CAUSE_CATEGORIES,
    confidence_threshold: str = DEFAULT_CONFIDENCE_THRESHOLD,
) -> EligibilityDecision:
    """Deterministically decide whether an incident may proceed to automated repair.

    This never calls a model. It only inspects the already-produced,
    already-grounded diagnosis.json. Any structural problem with the
    diagnosis itself (missing keys, bad enum values) is INVALID_DIAGNOSIS,
    not silently treated as ineligible-but-otherwise-fine.
    """
    if not isinstance(diagnosis, dict):
        return EligibilityDecision(RepairEligibility.INVALID_DIAGNOSIS, [f"diagnosis must be a JSON object, got {type(diagnosis).__name__}"])

    missing = [k for k in _REQUIRED_DIAGNOSIS_KEYS if k not in diagnosis]
    if missing:
        return EligibilityDecision(RepairEligibility.INVALID_DIAGNOSIS, [f"diagnosis is missing required keys: {missing}"])

    status = diagnosis["diagnosis_status"]
    if status not in ("DIAGNOSED", "NO_INCIDENT", "INSUFFICIENT_EVIDENCE"):
        return EligibilityDecision(RepairEligibility.INVALID_DIAGNOSIS, [f"unknown diagnosis_status {status!r}"])

    if status == "NO_INCIDENT":
        return EligibilityDecision(RepairEligibility.NO_REPAIR_NEEDED, ["validation passed; nothing to repair"])

    if status == "INSUFFICIENT_EVIDENCE":
        return EligibilityDecision(
            RepairEligibility.HUMAN_REVIEW_REQUIRED,
            ["diagnosis_status=INSUFFICIENT_EVIDENCE -- cannot safely automate a repair without adequate evidence"],
        )

    # status == "DIAGNOSED" from here on.
    reasons: list[str] = []

    if not diagnosis.get("evidence"):
        return EligibilityDecision(RepairEligibility.INVALID_DIAGNOSIS, ["DIAGNOSED diagnosis has no evidence"])

    category = diagnosis["root_cause_category"]
    if category not in eligible_root_cause_categories:
        return EligibilityDecision(
            RepairEligibility.HUMAN_REVIEW_REQUIRED,
            [
                f"root_cause_category {category!r} is not in the automated-repair-eligible set "
                f"{sorted(eligible_root_cause_categories)} -- requires human review"
            ],
        )

    confidence = diagnosis["confidence"]
    if confidence not in _CONFIDENCE_RANK:
        return EligibilityDecision(RepairEligibility.INVALID_DIAGNOSIS, [f"unknown confidence {confidence!r}"])
    if _CONFIDENCE_RANK[confidence] < _CONFIDENCE_RANK.get(confidence_threshold, 999):
        return EligibilityDecision(
            RepairEligibility.HUMAN_REVIEW_REQUIRED,
            [f"confidence {confidence!r} is below the required threshold {confidence_threshold!r}"],
        )

    recommended_fix = diagnosis.get("recommended_fix")
    if not recommended_fix or not recommended_fix.get("target_file"):
        return EligibilityDecision(
            RepairEligibility.HUMAN_REVIEW_REQUIRED,
            ["diagnosis has no recommended_fix.target_file to act on"],
        )

    target_file = recommended_fix["target_file"]
    if target_file not in allowed_target_files:
        return EligibilityDecision(
            RepairEligibility.HUMAN_REVIEW_REQUIRED,
            [f"recommended_fix.target_file {target_file!r} is not in the repair target allowlist"],
        )

    reasons.append(f"root_cause_category={category!r} eligible, confidence={confidence!r} meets threshold, target_file={target_file!r} allowlisted")
    return EligibilityDecision(RepairEligibility.ELIGIBLE_FOR_REPAIR, reasons)


# --- Repair plan schema -----------------------------------------------------


@dataclass(frozen=True)
class Patch:
    format: PatchFormat
    content: dict | str | None


@dataclass(frozen=True)
class RepairPlan:
    repair_decision: RepairDecision
    repair_type: RepairType
    incident_id: str
    diagnosis_reference: str
    root_cause_addressed: str
    target_file: str | None
    target_symbol_or_setting: str | None
    current_behavior: str
    proposed_behavior: str
    change_description: str
    patch: Patch | None
    files_expected_to_change: list[str]
    files_expected_not_to_change: list[str]
    verification_steps: list[str]
    rollback_description: str
    risk_level: RiskLevel
    assumptions: list[str]
    evidence_references: list[str]


_REQUIRED_KEYS = (
    "repair_decision", "repair_type", "incident_id", "diagnosis_reference", "root_cause_addressed",
    "target_file", "target_symbol_or_setting", "current_behavior", "proposed_behavior",
    "change_description", "patch", "files_expected_to_change", "files_expected_not_to_change",
    "verification_steps", "rollback_description", "risk_level", "assumptions", "evidence_references",
)
_REQUIRED_PATCH_KEYS = ("format", "content")

MAX_UNIFIED_DIFF_CHARS = 20_000
MAX_STRUCTURED_OPERATIONS = 10

# Files a repair plan must never be allowed to touch, regardless of what the
# model proposes -- checked independently of (and in addition to) the
# positive target allowlist, as a defense-in-depth backstop.
PROHIBITED_TARGET_FILES = frozenset(
    {
        "data/raw/loans.json",
        "data/raw/customers.json",
        "data/raw/payments.json",
        "src/validate_portfolio.py",
        "context/validation_rules.json",
        "data/processed/diagnosis.json",
        "data/processed/validation_results.json",
    }
)


def _require_keys(raw, keys: tuple, label: str) -> None:
    if not isinstance(raw, dict):
        raise RepairPlanValidationError(f"{label} must be a JSON object, got {type(raw).__name__}")
    missing = [k for k in keys if k not in raw]
    if missing:
        raise RepairPlanValidationError(f"{label} is missing required keys: {missing}")


def _parse_enum(enum_cls, value, field_name: str):
    try:
        return enum_cls(value)
    except ValueError:
        valid = [e.value for e in enum_cls]
        raise RepairPlanValidationError(f"invalid {field_name} {value!r}; must be one of {valid}")


def _require_string_list(value, field_name: str) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise RepairPlanValidationError(f"{field_name} must be a list of strings")
    return value


def _require_non_empty_string(value, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RepairPlanValidationError(f"{field_name} must be a non-empty string")
    return value


def _parse_patch(raw, *, repair_type: RepairType, target_descriptor: dict) -> Patch | None:
    if raw is None:
        return None
    _require_keys(raw, _REQUIRED_PATCH_KEYS, "patch")
    patch_format = _parse_enum(PatchFormat, raw["format"], "patch.format")
    content = raw["content"]

    if repair_type == RepairType.CONFIGURATION_CHANGE:
        if patch_format != PatchFormat.STRUCTURED_CONFIG_EDIT:
            raise RepairPlanValidationError(
                f"repair_type=CONFIGURATION_CHANGE requires patch.format=STRUCTURED_CONFIG_EDIT, got {patch_format.value}"
            )
        if not isinstance(content, dict) or "operations" not in content:
            raise RepairPlanValidationError("STRUCTURED_CONFIG_EDIT patch.content must be an object with an 'operations' list")
        operations = content["operations"]
        if not isinstance(operations, list) or not operations:
            raise RepairPlanValidationError("patch.content.operations must be a non-empty list")
        if len(operations) > MAX_STRUCTURED_OPERATIONS:
            raise RepairPlanValidationError(f"patch.content.operations exceeds the maximum of {MAX_STRUCTURED_OPERATIONS}")

        editable_fields = set(target_descriptor.get("editable_fields", []))
        allowed_values = target_descriptor.get("allowed_values", {})
        for op in operations:
            if not isinstance(op, dict) or "field" not in op or "value" not in op:
                raise RepairPlanValidationError("each operation must be an object with 'field' and 'value'")
            field_name = op["field"]
            if field_name not in editable_fields:
                raise RepairPlanValidationError(
                    f"field {field_name!r} is not editable for this target; editable fields: {sorted(editable_fields)}"
                )
            value_allowlist = allowed_values.get(field_name)
            if value_allowlist is not None and op["value"] not in value_allowlist:
                raise RepairPlanValidationError(
                    f"value {op['value']!r} is not an allowed value for field {field_name!r}; allowed: {value_allowlist}"
                )
        return Patch(format=patch_format, content=content)

    if repair_type == RepairType.CODE_CHANGE:
        if patch_format != PatchFormat.UNIFIED_DIFF:
            raise RepairPlanValidationError(
                f"repair_type=CODE_CHANGE requires patch.format=UNIFIED_DIFF, got {patch_format.value}"
            )
        if not isinstance(content, str) or not content.strip():
            raise RepairPlanValidationError("UNIFIED_DIFF patch.content must be a non-empty string")
        if len(content) > MAX_UNIFIED_DIFF_CHARS:
            raise RepairPlanValidationError(
                f"patch.content exceeds the maximum size of {MAX_UNIFIED_DIFF_CHARS} characters"
            )
        return Patch(format=patch_format, content=content)

    raise RepairPlanValidationError(f"unexpected repair_type {repair_type.value} for a non-null patch")


def parse_repair_plan(
    raw: dict,
    *,
    diagnosis: dict,
    allowed_targets: dict,
) -> RepairPlan:
    """Validate and parse a raw model-submitted repair plan dict.

    allowed_targets: {target_file_path: {"repair_type": str, "editable_fields": [...], "allowed_values": {...}}}
    -- the SAME registry used by the eligibility gate, loaded from
    context/repair_targets.json. Never derived from model input.

    Raises RepairPlanValidationError for any structural, enum, or grounding
    violation. Callers must not fall back to a partially-trusted plan.
    """
    _require_keys(raw, _REQUIRED_KEYS, "repair plan")

    repair_decision = _parse_enum(RepairDecision, raw["repair_decision"], "repair_decision")
    repair_type = _parse_enum(RepairType, raw["repair_type"], "repair_type")

    if repair_decision == RepairDecision.PROPOSE_REPAIR:
        if repair_type == RepairType.NONE:
            raise RepairPlanValidationError("repair_decision=PROPOSE_REPAIR requires a non-NONE repair_type")

        target_file = raw["target_file"]
        if not target_file or not isinstance(target_file, str):
            raise RepairPlanValidationError("repair_decision=PROPOSE_REPAIR requires a non-null target_file")
        if target_file in PROHIBITED_TARGET_FILES:
            raise RepairPlanValidationError(f"target_file {target_file!r} is a prohibited file and can never be a repair target")
        if target_file not in allowed_targets:
            raise RepairPlanValidationError(
                f"target_file {target_file!r} is not in the repair target allowlist; allowed: {sorted(allowed_targets)}"
            )
        target_descriptor = allowed_targets[target_file]
        if repair_type.value != target_descriptor["repair_type"]:
            raise RepairPlanValidationError(
                f"repair_type {repair_type.value!r} does not match the registered type for {target_file!r} "
                f"({target_descriptor['repair_type']!r})"
            )

        files_expected_to_change = _require_string_list(raw["files_expected_to_change"], "files_expected_to_change")
        if files_expected_to_change != [target_file]:
            raise RepairPlanValidationError(
                f"files_expected_to_change must be exactly [{target_file!r}] for this single-target MVP, "
                f"got {files_expected_to_change}"
            )

        patch = _parse_patch(raw["patch"], repair_type=repair_type, target_descriptor=target_descriptor)
        if patch is None:
            raise RepairPlanValidationError("repair_decision=PROPOSE_REPAIR requires a non-null patch")

        risk_level = _parse_enum(RiskLevel, raw["risk_level"], "risk_level")
        current_behavior = _require_non_empty_string(raw["current_behavior"], "current_behavior")
        proposed_behavior = _require_non_empty_string(raw["proposed_behavior"], "proposed_behavior")
        change_description = _require_non_empty_string(raw["change_description"], "change_description")
        rollback_description = _require_non_empty_string(raw["rollback_description"], "rollback_description")
        root_cause_addressed = _require_non_empty_string(raw["root_cause_addressed"], "root_cause_addressed")
        diagnosis_reference = _require_non_empty_string(raw["diagnosis_reference"], "diagnosis_reference")
        incident_id = _require_non_empty_string(raw["incident_id"], "incident_id")
        target_symbol_or_setting = raw["target_symbol_or_setting"]

        verification_steps = _require_string_list(raw["verification_steps"], "verification_steps")
        if not verification_steps:
            raise RepairPlanValidationError("verification_steps must not be empty for a proposed repair")

        files_expected_not_to_change = _require_string_list(
            raw["files_expected_not_to_change"], "files_expected_not_to_change"
        )
        assumptions = _require_string_list(raw["assumptions"], "assumptions")

        evidence_references = _require_string_list(raw["evidence_references"], "evidence_references")
        if not evidence_references:
            raise RepairPlanValidationError("evidence_references must not be empty for a proposed repair")
        known_evidence_refs = {e.get("source_reference") for e in diagnosis.get("evidence", [])}
        unknown_refs = [r for r in evidence_references if r not in known_evidence_refs]
        if unknown_refs:
            raise RepairPlanValidationError(
                f"evidence_references contains entries not present in the diagnosis's own evidence: {unknown_refs}"
            )

        return RepairPlan(
            repair_decision=repair_decision,
            repair_type=repair_type,
            incident_id=incident_id,
            diagnosis_reference=diagnosis_reference,
            root_cause_addressed=root_cause_addressed,
            target_file=target_file,
            target_symbol_or_setting=target_symbol_or_setting,
            current_behavior=current_behavior,
            proposed_behavior=proposed_behavior,
            change_description=change_description,
            patch=patch,
            files_expected_to_change=files_expected_to_change,
            files_expected_not_to_change=files_expected_not_to_change,
            verification_steps=verification_steps,
            rollback_description=rollback_description,
            risk_level=risk_level,
            assumptions=assumptions,
            evidence_references=evidence_references,
        )

    # NO_SAFE_REPAIR or HUMAN_REVIEW_REQUIRED: no patch, no target.
    if raw["target_file"] is not None or raw["patch"] is not None:
        raise RepairPlanValidationError(
            f"repair_decision={repair_decision.value} must have target_file=null and patch=null"
        )
    change_description = _require_non_empty_string(raw["change_description"], "change_description")
    rollback_description = raw.get("rollback_description") or "not applicable -- no change was made"
    risk_level = _parse_enum(RiskLevel, raw["risk_level"], "risk_level")

    return RepairPlan(
        repair_decision=repair_decision,
        repair_type=RepairType.NONE,
        incident_id=_require_non_empty_string(raw["incident_id"], "incident_id"),
        diagnosis_reference=_require_non_empty_string(raw["diagnosis_reference"], "diagnosis_reference"),
        root_cause_addressed=raw.get("root_cause_addressed") or "",
        target_file=None,
        target_symbol_or_setting=None,
        current_behavior=raw.get("current_behavior") or "",
        proposed_behavior=raw.get("proposed_behavior") or "",
        change_description=change_description,
        patch=None,
        files_expected_to_change=[],
        files_expected_not_to_change=_require_string_list(raw["files_expected_not_to_change"], "files_expected_not_to_change"),
        verification_steps=[],
        rollback_description=rollback_description,
        risk_level=risk_level,
        assumptions=_require_string_list(raw["assumptions"], "assumptions"),
        evidence_references=_require_string_list(raw["evidence_references"], "evidence_references"),
    )


def build_blocked_repair_plan(decision_reason: str, *, incident_id: str, diagnosis_reference: str) -> RepairPlan:
    """Deterministic HUMAN_REVIEW_REQUIRED/NO_SAFE_REPAIR plan -- no model call."""
    return RepairPlan(
        repair_decision=RepairDecision.HUMAN_REVIEW_REQUIRED,
        repair_type=RepairType.NONE,
        incident_id=incident_id,
        diagnosis_reference=diagnosis_reference,
        root_cause_addressed="",
        target_file=None,
        target_symbol_or_setting=None,
        current_behavior="",
        proposed_behavior="",
        change_description=decision_reason,
        patch=None,
        files_expected_to_change=[],
        files_expected_not_to_change=[],
        verification_steps=[],
        rollback_description="not applicable -- no change was made",
        risk_level=RiskLevel.LOW,
        assumptions=[],
        evidence_references=[],
    )


def build_no_repair_needed_plan(*, incident_id: str, diagnosis_reference: str) -> RepairPlan:
    """Deterministic NO_SAFE_REPAIR plan for a clean (NO_INCIDENT) diagnosis -- no model call."""
    return RepairPlan(
        repair_decision=RepairDecision.NO_SAFE_REPAIR,
        repair_type=RepairType.NONE,
        incident_id=incident_id,
        diagnosis_reference=diagnosis_reference,
        root_cause_addressed="not applicable -- no incident",
        target_file=None,
        target_symbol_or_setting=None,
        current_behavior="",
        proposed_behavior="",
        change_description="Validation passed; there is no incident to repair.",
        patch=None,
        files_expected_to_change=[],
        files_expected_not_to_change=[],
        verification_steps=[],
        rollback_description="not applicable -- no change was made",
        risk_level=RiskLevel.LOW,
        assumptions=[],
        evidence_references=[],
    )


def repair_plan_to_dict(plan: RepairPlan) -> dict:
    """Serialize a RepairPlan to a JSON-compatible dict (enums -> their .value)."""
    d = asdict(plan)
    d["repair_decision"] = plan.repair_decision.value
    d["repair_type"] = plan.repair_type.value
    d["risk_level"] = plan.risk_level.value
    if plan.patch is not None:
        d["patch"] = {"format": plan.patch.format.value, "content": plan.patch.content}
    else:
        d["patch"] = None
    return d
