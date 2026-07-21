"""Shared reconciliation-check helper for the lifecycle ETL validators.

Each validate_*.py module for the 5 curated pipelines independently
recomputes expected metrics in pandas and compares them against a curated
Spark ETL's output -- this one function is the single place that scalar
comparison logic lives, so a fix only needs to happen once.

Three edge cases are handled explicitly here after being found (and, in
several validators, gotten wrong) during review:
- Both sides missing (None or NaN, e.g. an average over zero rows on both the
  Spark and pandas side) -- PASS, they agree on "undefined."
- Only ONE side missing -- a real, reportable FAIL, not a crash. A naive
  `float(None)` on the missing side raises TypeError instead of surfacing
  this as the mismatch it actually is.
- Which tolerance applies (count/currency/rate) is resolved from the rule's
  OWN declared `tolerance_type` (already present in every
  context/validations/*.json rule), never re-derived by guessing from the
  metric's column name -- a substring heuristic like `"amount" in column`
  is exactly what missed `total_balance_at_default` (contains neither
  "amount" nor "principal") in one validator during this build.
"""

from __future__ import annotations

import math


def is_missing(value) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def reconciliation_check(rule: dict, tolerances: dict, expected, actual) -> dict:
    tolerance = tolerances[rule["tolerance_type"]]
    expected_missing = is_missing(expected)
    actual_missing = is_missing(actual)

    if expected_missing and actual_missing:
        return {
            "id": rule["id"],
            "description": rule["description"],
            "status": "PASS",
            "expected": None,
            "actual": None,
            "difference": None,
            "details": None,
        }

    if expected_missing != actual_missing:
        return {
            "id": rule["id"],
            "description": rule["description"],
            "status": "FAIL",
            "expected": None if expected_missing else round(float(expected), 4),
            "actual": None if actual_missing else round(float(actual), 4),
            "difference": None,
            "details": "one side is missing/undefined (null or NaN) while the other has a real value",
        }

    expected_f = round(float(expected), 4)
    actual_f = round(float(actual), 4)
    difference = round(actual_f - expected_f, 4)
    status = "PASS" if abs(difference) <= tolerance else "FAIL"
    return {
        "id": rule["id"],
        "description": rule["description"],
        "status": status,
        "expected": expected_f,
        "actual": actual_f,
        "difference": difference,
        "details": None,
    }
