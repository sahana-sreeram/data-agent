"""End-to-end self-healing CLI: eligibility gate -> repair planning -> policy
validation -> isolated apply -> deterministic verification -> promotion.

Composes apply_repair.run_apply_repair and verify_repair.run_verify_repair.
Being blocked for human review, or finding no incident, is a normal,
successful run of this command -- only a genuine application failure
(missing artifacts, model/API failure, malformed model output, or an
internal verification error) exits nonzero. A repair that was applied but
failed to verify also exits nonzero, since the pipeline is still broken and
that's worth surfacing via exit code.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Callable

from src.legacy.apply_repair import (
    DEFAULT_CONFIDENCE_THRESHOLD,
    DEFAULT_REPAIR_TARGETS_FILE,
    REPAIR_MODEL_ENV_VAR,
    ApplyRepairError,
    load_scenario_manifest,
    print_repair_result,
    run_apply_repair,
    write_json_file,
)
from src.model_client import DiagnosisModelClient, OpenAIDiagnosisModelClient
from src.legacy.verify_repair import VerifyRepairError, print_verification, run_verify_repair


def run_self_healing(
    manifest: dict,
    model_client_factory: Callable[[], DiagnosisModelClient],
    *,
    repair_targets_file: str = DEFAULT_REPAIR_TARGETS_FILE,
    confidence_threshold: str = DEFAULT_CONFIDENCE_THRESHOLD,
    output_dir: Path,
) -> dict:
    """Run the full flow, writing all three artifacts. Returns the combined outcome dict."""
    plan_dict, result = run_apply_repair(
        manifest,
        model_client_factory,
        repair_targets_file=repair_targets_file,
        confidence_threshold=confidence_threshold,
    )
    write_json_file(output_dir / "repair_plan.json", plan_dict)
    write_json_file(output_dir / "repair_result.json", result)

    verification = run_verify_repair(manifest, plan_dict, result)
    write_json_file(output_dir / "repair_verification.json", verification)

    return {"repair_plan": plan_dict, "repair_result": result, "repair_verification": verification}


def print_summary(result: dict, verification: dict) -> None:
    print_repair_result(result)
    print()
    print_verification(verification)


def parse_args(argv: list = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the full self-healing workflow (apply + verify) for one incident's scenario manifest."
    )
    parser.add_argument("--scenario-manifest-file", type=str, required=True)
    parser.add_argument("--repair-targets-file", type=str, default=DEFAULT_REPAIR_TARGETS_FILE)
    parser.add_argument(
        "--confidence-threshold", type=str, default=DEFAULT_CONFIDENCE_THRESHOLD, choices=["LOW", "MEDIUM", "HIGH"]
    )
    parser.add_argument("--output-dir", type=str, default=None, help="Defaults to the manifest file's own directory.")
    parser.add_argument("--model", type=str, default=None)
    return parser.parse_args(argv)


def main(argv: list = None) -> None:
    args = parse_args(argv)
    manifest_path = Path(args.scenario_manifest_file)
    manifest = load_scenario_manifest(manifest_path)
    output_dir = Path(args.output_dir) if args.output_dir else manifest_path.parent

    model_name = args.model or os.environ.get(REPAIR_MODEL_ENV_VAR)

    def model_client_factory() -> DiagnosisModelClient:
        return OpenAIDiagnosisModelClient(model=model_name) if model_name else OpenAIDiagnosisModelClient()

    try:
        outcome = run_self_healing(
            manifest,
            model_client_factory,
            repair_targets_file=args.repair_targets_file,
            confidence_threshold=args.confidence_threshold,
            output_dir=output_dir,
        )
    except (ApplyRepairError, VerifyRepairError) as exc:
        print(f"Self-healing workflow failed: {exc}")
        raise SystemExit(1)

    print_summary(outcome["repair_result"], outcome["repair_verification"])

    verification_status = outcome["repair_verification"]["verification_status"]
    if verification_status not in ("VERIFIED", "BLOCKED"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
