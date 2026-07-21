"""Deterministic post-repair verification and promotion.

Reruns the ETL (using the PATCHED code/config from the isolated workspace)
and validation (using the real, unmodified validator) against real source
data, compares before/after, and ONLY on full success promotes the patched
target file and freshly-computed outputs into the real repository. On any
failure, the isolated workspace is discarded and the real repository is left
completely untouched.

This module -- not the repair agent -- is the sole authority on whether a
repair is VERIFIED.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import shutil
from pathlib import Path

DEFAULT_CONFIDENCE_THRESHOLD = "HIGH"  # unused here; kept for CLI symmetry with apply_repair


class VerifyRepairError(Exception):
    """Application-level failure: missing artifacts or an internal rerun error."""


def load_json(path: Path, label: str) -> dict:
    if not path.exists():
        raise VerifyRepairError(f"{label} file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _sha256_of_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _workspace_path(workspace_dir: Path, target_file: str) -> Path:
    """Map a target_file (relative or absolute) to its mirrored location under workspace_dir.

    Mirrors apply_repair._workspace_path -- joining a Path with an absolute
    string discards the base, so the leading separator is stripped first.
    """
    return workspace_dir / target_file.lstrip("/")


def _load_patched_transform_module(workspace_dir: Path):
    """Dynamically import the patched copy of src/transform.py as an ISOLATED module object.

    Never touches sys.modules['src.transform'] -- the real, installed module
    is completely unaffected. This is how a CODE_CHANGE's patched behavior
    gets genuinely exercised before promotion.
    """
    patched_path = workspace_dir / "src" / "transform.py"
    spec = importlib.util.spec_from_file_location("patched_transform_for_verification", patched_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _rerun_one_row_per_payment(manifest: dict, workspace_dir: Path, repair_result: dict) -> tuple[dict, dict]:
    from src.transform import compute_portfolio_summary, load_business_rules, load_loans, load_payments
    from src.validate_portfolio import load_validation_rules, validate_portfolio

    rerun_cfg = manifest["rerun"]
    loans_df = load_loans(Path(rerun_cfg["loans_file"]))
    payments_df = load_payments(Path(rerun_cfg["payments_file"]))

    target_file = repair_result["target_file"]
    if target_file == manifest.get("pipeline_configuration_file"):
        patched_config = json.loads(_workspace_path(workspace_dir, target_file).read_text(encoding="utf-8"))
        etl_business_rules_file = patched_config["business_rules_file"]
    else:
        etl_business_rules_file = rerun_cfg["validation_business_rules_file"]

    etl_business_rules = load_business_rules(Path(etl_business_rules_file))
    summary = compute_portfolio_summary(loans_df, payments_df, rerun_cfg["as_of_date"], etl_business_rules)

    # Validation always checks against the authoritative/adopted rule from the
    # manifest -- NEVER against whatever the (possibly still-wrong) patched ETL
    # config says to use. Using the ETL's own rule here would make validation
    # merely check "does the ETL agree with itself", which a repair that fails
    # to fix the real problem could still pass.
    validation_business_rules = load_business_rules(Path(rerun_cfg["validation_business_rules_file"]))
    validation_rules = load_validation_rules(Path(rerun_cfg["validation_rules_file"]))
    validation_results = validate_portfolio(loans_df, payments_df, summary, validation_business_rules, validation_rules)
    return summary, validation_results


def _rerun_payment_events(manifest: dict, workspace_dir: Path, repair_result: dict) -> tuple[dict, dict]:
    from src.transform import load_business_rules, load_loans, load_payment_events
    from src.validate_portfolio import load_validation_rules, validate_portfolio_from_payment_events

    rerun_cfg = manifest["rerun"]
    loans_df = load_loans(Path(rerun_cfg["loans_file"]))
    payment_events_df = load_payment_events(Path(rerun_cfg["payment_events_file"]))
    business_rules = load_business_rules(Path(rerun_cfg["business_rules_file"]))

    patched_module = _load_patched_transform_module(workspace_dir)
    etl_function = getattr(patched_module, manifest["etl_function_name"])
    summary = etl_function(loans_df, payment_events_df, rerun_cfg["as_of_date"], business_rules)

    validation_rules = load_validation_rules(Path(rerun_cfg["validation_rules_file"]))
    validation_results = validate_portfolio_from_payment_events(
        loans_df, payment_events_df, summary, business_rules, validation_rules
    )
    return summary, validation_results


def _rerun_loan_payment_join(manifest: dict, workspace_dir: Path, repair_result: dict) -> tuple[dict, dict]:
    from src.transform import load_business_rules, load_loans, load_payments
    from src.validate_portfolio import load_validation_rules, validate_portfolio_with_join_profile

    rerun_cfg = manifest["rerun"]
    loans_df = load_loans(Path(rerun_cfg["loans_file"]))
    payments_df = load_payments(Path(rerun_cfg["payments_file"]))
    business_rules = load_business_rules(Path(rerun_cfg["business_rules_file"]))

    patched_module = _load_patched_transform_module(workspace_dir)
    etl_function = getattr(patched_module, manifest["etl_function_name"])
    summary = etl_function(loans_df, payments_df, rerun_cfg["as_of_date"], business_rules)

    validation_rules = load_validation_rules(Path(rerun_cfg["validation_rules_file"]))
    validation_results = validate_portfolio_with_join_profile(
        loans_df, payments_df, summary, business_rules, validation_rules
    )
    return summary, validation_results


_RERUN_DISPATCH = {
    "one_row_per_payment": _rerun_one_row_per_payment,
    "payment_events": _rerun_payment_events,
    "loan_payment_join": _rerun_loan_payment_join,
}


def _run_pytest(test_files: list) -> str:
    import pytest

    exit_code = pytest.main(["-q", *test_files])
    return "PASS" if int(exit_code) == 0 else "FAIL"


def _raw_data_hashes(manifest: dict) -> dict:
    rerun_cfg = manifest["rerun"]
    paths = [rerun_cfg["loans_file"]]
    paths.append(rerun_cfg.get("payments_file") or rerun_cfg.get("payment_events_file"))
    return {p: _sha256_of_file(Path(p)) for p in paths}


def _protected_file_hashes(manifest: dict) -> dict:
    protected = ["context/validation_rules.json", "src/validate_portfolio.py", manifest["diagnosis_file"]]
    return {p: _sha256_of_file(Path(p)) for p in protected if Path(p).exists()}


def _checks_by_id(validation_results: dict) -> dict:
    return {c["id"]: c for c in validation_results.get("checks", [])}


def _promote(manifest: dict, repair_result: dict, workspace_dir: Path, summary: dict, validation_results: dict) -> list:
    """Copy the verified-good workspace outputs over the real repository. Only called after full verification passes."""
    promoted: list = []

    target_file = repair_result["target_file"]
    real_target_path = Path(target_file)
    shutil.copy2(_workspace_path(workspace_dir, target_file), real_target_path)
    promoted.append(str(real_target_path))

    scenario_dir = Path(manifest["diagnosis_file"]).parent
    summary_path = scenario_dir / "portfolio_summary.json"
    validation_path = scenario_dir / "validation_results.json"
    pipeline_run_path = scenario_dir / "pipeline_run.json"

    for path, data in ((summary_path, summary), (validation_path, validation_results)):
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
            f.write("\n")
        promoted.append(str(path))

    pipeline_run = {
        "as_of_date": manifest["rerun"].get("as_of_date"),
        "etl_status": "SUCCESS",
        "etl_error": None,
        "validation_status": validation_results["overall_status"],
        "validation_error": None,
        "overall_status": "SUCCESS" if validation_results["overall_status"] == "PASS" else "FAILURE",
        "repaired_by": "src.verify_repair",
    }
    with pipeline_run_path.open("w", encoding="utf-8") as f:
        json.dump(pipeline_run, f, indent=2)
        f.write("\n")
    promoted.append(str(pipeline_run_path))

    return promoted


def run_verify_repair(manifest: dict, repair_plan: dict, repair_result: dict) -> dict:
    """Run the full verification flow. Returns the repair_verification dict.

    Discards the isolated workspace (if any) on any failure; promotes it to
    the real repository only when every deterministic check passes.
    """
    if repair_result["repair_status"] != "APPLIED":
        return {
            "verification_status": "BLOCKED",
            "diagnosis_status": None,
            "repair_status": repair_result["repair_status"],
            "tests": {"targeted": "NOT_RUN", "full_relevant_suite": "NOT_RUN"},
            "etl_status_after": "NOT_RUN",
            "validation_before": None,
            "validation_after": "NOT_RUN",
            "failed_checks_before": [],
            "failed_checks_after": [],
            "metrics_before": {},
            "metrics_after": {},
            "changed_files": [],
            "unchanged_protected_files_verified": True,
            "raw_data_unchanged": True,
            "rollback_performed": False,
            "summary": f"Nothing to verify -- repair_status was {repair_result['repair_status']!r}, not APPLIED.",
        }

    workspace_dir = Path(repair_result["workspace_dir"])
    validation_before = load_json(Path(manifest["validation_results_file"]), "validation results (before)")
    metrics_before = load_json(Path(manifest["portfolio_summary_file"]), "portfolio summary (before)")

    raw_hashes_before = _raw_data_hashes(manifest)
    protected_hashes_before = _protected_file_hashes(manifest)

    rerun_kind = manifest["rerun"]["kind"]
    if rerun_kind not in _RERUN_DISPATCH:
        raise VerifyRepairError(f"unknown rerun kind {rerun_kind!r}")

    try:
        metrics_after, validation_after = _RERUN_DISPATCH[rerun_kind](manifest, workspace_dir, repair_result)
        etl_status_after = "SUCCESS"
    except Exception as exc:  # noqa: BLE001 -- rerun failure is a verification outcome, not a crash
        shutil.rmtree(workspace_dir, ignore_errors=True)
        return {
            "verification_status": "NOT_VERIFIED",
            "diagnosis_status": "DIAGNOSED",
            "repair_status": repair_result["repair_status"],
            "tests": {"targeted": "NOT_RUN", "full_relevant_suite": "NOT_RUN"},
            "etl_status_after": "FAILED",
            "validation_before": validation_before.get("overall_status"),
            "validation_after": "NOT_RUN",
            "failed_checks_before": [c["id"] for c in validation_before.get("checks", []) if c["status"] == "FAIL"],
            "failed_checks_after": [],
            "metrics_before": metrics_before,
            "metrics_after": {},
            "changed_files": [],
            "unchanged_protected_files_verified": True,
            "raw_data_unchanged": True,
            "rollback_performed": True,
            "summary": f"ETL rerun against the patched workspace failed: {exc}",
        }

    targeted_tests = manifest["test_inventory"][:1]
    full_suite = manifest["test_inventory"]
    targeted_status = _run_pytest(targeted_tests)
    full_suite_status = _run_pytest(full_suite)

    raw_hashes_after = _raw_data_hashes(manifest)
    protected_hashes_after = _protected_file_hashes(manifest)
    raw_data_unchanged = raw_hashes_before == raw_hashes_after
    protected_files_unchanged = protected_hashes_before == protected_hashes_after

    checks_before = _checks_by_id(validation_before)
    checks_after = _checks_by_id(validation_after)
    failed_before = [check_id for check_id, c in checks_before.items() if c["status"] == "FAIL"]
    failed_after = [check_id for check_id, c in checks_after.items() if c["status"] == "FAIL"]

    previously_failed_now_pass = all(checks_after.get(check_id, {}).get("status") == "PASS" for check_id in failed_before)
    previously_passed_still_pass = all(
        checks_after.get(check_id, {}).get("status") == "PASS"
        for check_id, c in checks_before.items()
        if c["status"] == "PASS"
    )

    all_checks_pass = (
        targeted_status == "PASS"
        and full_suite_status == "PASS"
        and etl_status_after == "SUCCESS"
        and validation_after["overall_status"] == "PASS"
        and previously_failed_now_pass
        and previously_passed_still_pass
        and raw_data_unchanged
        and protected_files_unchanged
    )

    if all_checks_pass:
        changed_files = _promote(manifest, repair_result, workspace_dir, metrics_after, validation_after)
        shutil.rmtree(workspace_dir, ignore_errors=True)
        verification_status = "VERIFIED"
        rollback_performed = False
        summary = "All deterministic checks passed; repair promoted to the real repository."
    else:
        changed_files = []
        shutil.rmtree(workspace_dir, ignore_errors=True)
        verification_status = "NOT_VERIFIED"
        rollback_performed = True
        summary = "One or more deterministic checks failed; isolated workspace discarded, repository left untouched."

    return {
        "verification_status": verification_status,
        "diagnosis_status": "DIAGNOSED",
        "repair_status": repair_result["repair_status"],
        "tests": {"targeted": targeted_status, "full_relevant_suite": full_suite_status},
        "etl_status_after": etl_status_after,
        "validation_before": validation_before.get("overall_status"),
        "validation_after": validation_after.get("overall_status"),
        "failed_checks_before": failed_before,
        "failed_checks_after": failed_after,
        "metrics_before": metrics_before,
        "metrics_after": metrics_after,
        "changed_files": changed_files,
        "unchanged_protected_files_verified": protected_files_unchanged,
        "raw_data_unchanged": raw_data_unchanged,
        "rollback_performed": rollback_performed,
        "summary": summary,
    }


def write_json_file(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def print_verification(result: dict) -> None:
    print("Repair verification")
    print(f"  verification_status: {result['verification_status']}")
    print(f"  validation_before:    {result['validation_before']}")
    print(f"  validation_after:     {result['validation_after']}")
    print(f"  tests:                targeted={result['tests']['targeted']} full={result['tests']['full_relevant_suite']}")
    print(f"  rollback_performed:   {result['rollback_performed']}")
    print(f"  summary:              {result['summary']}")


def parse_args(argv: list = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deterministically verify (and, if fully passing, promote) an applied repair.")
    parser.add_argument("--scenario-manifest-file", type=str, required=True)
    parser.add_argument("--repair-plan-file", type=str, default=None, help="Defaults to <manifest dir>/repair_plan.json")
    parser.add_argument("--repair-result-file", type=str, default=None, help="Defaults to <manifest dir>/repair_result.json")
    parser.add_argument("--output-dir", type=str, default=None, help="Defaults to the manifest file's own directory.")
    return parser.parse_args(argv)


def main(argv: list = None) -> None:
    args = parse_args(argv)
    manifest_path = Path(args.scenario_manifest_file)
    manifest = load_json(manifest_path, "scenario manifest")
    output_dir = Path(args.output_dir) if args.output_dir else manifest_path.parent

    repair_plan_path = Path(args.repair_plan_file) if args.repair_plan_file else output_dir / "repair_plan.json"
    repair_result_path = Path(args.repair_result_file) if args.repair_result_file else output_dir / "repair_result.json"

    try:
        repair_plan = load_json(repair_plan_path, "repair plan")
        repair_result = load_json(repair_result_path, "repair result")
        result = run_verify_repair(manifest, repair_plan, repair_result)
    except VerifyRepairError as exc:
        print(f"Verification failed: {exc}")
        raise SystemExit(1)

    write_json_file(output_dir / "repair_verification.json", result)
    print_verification(result)

    if result["verification_status"] not in ("VERIFIED", "BLOCKED"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
