"""Tests for src/generate_upstream_events.py -- run only the "small" profile (fast,
deterministic, ~1000 customers). "demo"/"large" are exercised manually (see
demo/services/README.md and the module's own docstring for measured timings), not in the default
test suite -- they're minutes-long by design.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.events_to_lifecycle_tables import build_lifecycle_tables_from_events
from src.generate_upstream_events import PROFILES, run_scale_generation
from src.validate_lifecycle_raw import validate_lifecycle_raw


def test_small_profile_completes_and_reports_every_service(tmp_path):
    report = run_scale_generation("small", seed=42, as_of_date="2026-07-20", output="local", chunk_size=500, local_dir=tmp_path)

    assert report["batch_count"] == 2  # 1000 customers / 500 per chunk
    assert report["total_events"] > 0
    assert set(report["events_by_service"]) == {
        "marketing_service", "application_service", "underwriting_service", "loan_service", "payment_service", "risk_service",
    }


def test_small_profile_is_deterministic(tmp_path):
    report1 = run_scale_generation("small", seed=42, as_of_date="2026-07-20", output="local", chunk_size=500, local_dir=tmp_path / "a")
    report2 = run_scale_generation("small", seed=42, as_of_date="2026-07-20", output="local", chunk_size=500, local_dir=tmp_path / "b")
    assert report1["total_events"] == report2["total_events"]


def test_multi_batch_ids_never_collide_and_reconstruct_cleanly(tmp_path):
    """The whole point of batch namespacing (demo/services/common/seeding.generate_namespaced_batch)
    -- two batches of customers must never produce the same customer_id/loan_id/etc., and the
    combined output must still pass the real raw-table validator."""
    run_scale_generation("small", seed=42, as_of_date="2026-07-20", output="local", chunk_size=500, local_dir=tmp_path)

    tables = build_lifecycle_tables_from_events(local_dir=tmp_path)
    assert tables["customers"]["customer_id"].is_unique
    assert tables["loans"]["loan_id"].is_unique

    import json

    business_rules = json.loads(Path("context/business_rules.json").read_text())
    validation_rules = json.loads(Path("context/validations/lifecycle_raw.json").read_text())
    result = validate_lifecycle_raw(tables, business_rules, validation_rules)
    assert result["overall_status"] == "PASS", [c for c in result["checks"] if c["status"] == "FAIL"]


def test_v2_contract_version_propagates_through_scale_generation(tmp_path):
    run_scale_generation("small", seed=42, as_of_date="2026-07-20", output="local", chunk_size=500, local_dir=tmp_path, contract_version="v2")
    tables = build_lifecycle_tables_from_events(local_dir=tmp_path)
    assert "SETTLED" in set(tables["payment_events"]["payment_status"])
    assert "PAID" not in set(tables["payment_events"]["payment_status"])


def test_profiles_are_ordered_small_to_large():
    assert PROFILES["small"] < PROFILES["demo"] < PROFILES["large"]


def test_unknown_profile_raises():
    with pytest.raises(KeyError):
        run_scale_generation("nonexistent", seed=42, as_of_date="2026-07-20", output="local")
