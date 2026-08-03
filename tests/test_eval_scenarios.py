"""Guards that keep src/eval_scenarios.py's BugScenario catalog in sync with the real ETL
files it targets, and that REFUSAL_CASES stay well-formed. Fast, no Spark/S3/model calls --
these fail loudly the moment a scenario drifts out of sync, instead of at eval run time.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.eval_scenarios import BUG_SCENARIOS, REFUSAL_CASES, UPSTREAM_CONTRACT_SCENARIOS
from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY
from src.repair_models import RepairEligibility


@pytest.mark.parametrize("scenario", BUG_SCENARIOS, ids=lambda s: s.name)
def test_find_string_occurs_exactly_once_in_the_real_target_file(scenario):
    source = Path(scenario.target_file).read_text()
    assert source.count(scenario.find) == 1, (
        f"scenario {scenario.name!r} expects exactly 1 occurrence of its `find` string in "
        f"{scenario.target_file}; the real file has drifted out of sync with this scenario"
    )


@pytest.mark.parametrize("scenario", BUG_SCENARIOS, ids=lambda s: s.name)
def test_applying_and_reverting_the_bug_round_trips_to_the_original_source(scenario):
    source = Path(scenario.target_file).read_text()
    buggy = source.replace(scenario.find, scenario.replace, 1)
    assert buggy != source
    reverted = buggy.replace(scenario.replace, scenario.find, 1)
    assert reverted == source


def test_bug_class_is_one_of_the_two_known_values():
    for scenario in BUG_SCENARIOS:
        assert scenario.bug_class in ("ETL_LOGIC_JOIN", "BUSINESS_RULE_MISMATCH")


def test_scenarios_cover_at_least_two_pipelines_per_bug_class():
    by_class: dict = {}
    for scenario in BUG_SCENARIOS:
        by_class.setdefault(scenario.bug_class, set()).add(scenario.pipeline_name)
    for bug_class, pipelines in by_class.items():
        assert len(pipelines) >= 2, f"{bug_class} only covers {pipelines} -- no by-bug-class-and-pipeline signal"


@pytest.mark.parametrize("scenario", UPSTREAM_CONTRACT_SCENARIOS, ids=lambda s: s.name)
def test_upstream_contract_scenario_targets_a_real_registered_pipeline(scenario):
    assert scenario.pipeline_name in PIPELINE_REGISTRY
    assert "payment_events" in PIPELINE_REGISTRY[scenario.pipeline_name].raw_tables


def test_upstream_contract_scenario_expects_source_contract_change():
    for scenario in UPSTREAM_CONTRACT_SCENARIOS:
        assert scenario.expected_root_cause_category == "SOURCE_CONTRACT_CHANGE"
        assert scenario.contract_version == "v2"
        assert scenario.num_customers > 0


def test_refusal_cases_are_well_formed():
    assert len(REFUSAL_CASES) >= 2
    expected_decisions = {case[3] for case in REFUSAL_CASES}
    assert expected_decisions & {RepairEligibility.HUMAN_REVIEW_REQUIRED}
    assert expected_decisions & {RepairEligibility.ELIGIBLE_FOR_REPAIR, RepairEligibility.NO_REPAIR_NEEDED}
    for name, diagnosis, allowed_targets, expected_decision in REFUSAL_CASES:
        assert isinstance(name, str) and name
        assert isinstance(diagnosis, dict)
        assert isinstance(allowed_targets, set) and allowed_targets
        assert isinstance(expected_decision, RepairEligibility)
