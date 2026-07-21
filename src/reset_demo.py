"""Reset the self-healing demo scenarios back to their known-good, healed state.

Running the `ask.py` demo against `settled_rule_adopted` or `incorrect_join`
requires deliberately breaking them first (a stale config pointer, or a
reverted ETL join fix), then letting `ask.py` diagnose and repair them live.
This script undoes exactly that -- restoring the handful of files those two
demos mutate from a one-time snapshot taken while both were healthy
(`demo_snapshot/`), and clearing the transient diagnosis/repair/answer
artifacts so each run starts clean.

`settled_bug` is untouched: it's permanently broken by design and never
gets healed, so there's nothing to reset. The clean baseline
(`data/processed/`) is also untouched: `ask.py` never modifies it.
`payment_events_cardinality` is no longer part of the regularly-reset demo
rotation (superseded by `incorrect_join` as the headline code-repair demo --
see README.md) -- it's left alone, still healthy, not reset by this script.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

DEFAULT_SNAPSHOT_DIR = Path("demo_snapshot")

# Files the two re-breakable demos actually mutate. Restored byte-for-byte
# from the snapshot. src/transform.py is shared by both incorrect_join's and
# payment_events_cardinality's ETL functions -- restoring it restores both to
# their current healthy state regardless of which demo mutated it.
RESTORED_PATHS = [
    "src/transform.py",
    "data/scenarios/settled_rule_adopted/pipeline_config.json",
    "data/scenarios/settled_rule_adopted/portfolio_summary.json",
    "data/scenarios/settled_rule_adopted/validation_results.json",
    "data/scenarios/settled_rule_adopted/pipeline_run.json",
    "data/scenarios/incorrect_join/portfolio_summary.json",
    "data/scenarios/incorrect_join/validation_results.json",
    "data/scenarios/incorrect_join/pipeline_run.json",
]

# Per-run artifacts written by diagnose_incident/apply_repair/verify_repair/ask
# -- regenerated fresh on every run, so it's fine to just delete them.
TRANSIENT_ARTIFACTS = [
    f"data/scenarios/{scenario}/{name}"
    for scenario in ("settled_rule_adopted", "incorrect_join", "settled_bug")
    for name in ("diagnosis.json", "repair_plan.json", "repair_result.json", "repair_verification.json", "answer.json")
] + ["data/processed/answer.json"]


class ResetDemoError(Exception):
    """Raised when the snapshot is missing or incomplete."""


def create_snapshot(snapshot_dir: Path = DEFAULT_SNAPSHOT_DIR) -> list[str]:
    """One-time: capture the CURRENT state as the reset target. Only run this when everything is healthy."""
    captured = []
    for rel_path in RESTORED_PATHS:
        source = Path(rel_path)
        if not source.exists():
            raise ResetDemoError(f"cannot snapshot missing file: {rel_path}")
        destination = snapshot_dir / rel_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        captured.append(rel_path)
    return captured


def reset_demo(snapshot_dir: Path = DEFAULT_SNAPSHOT_DIR) -> tuple[list[str], list[str]]:
    """Restore the mutable demo files from the snapshot and clear transient artifacts."""
    restored = []
    for rel_path in RESTORED_PATHS:
        snapshot_path = snapshot_dir / rel_path
        if not snapshot_path.exists():
            raise ResetDemoError(f"missing snapshot for {rel_path} -- run with --create-snapshot first")
        destination = Path(rel_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(snapshot_path, destination)
        restored.append(rel_path)

    removed = []
    for rel_path in TRANSIENT_ARTIFACTS:
        path = Path(rel_path)
        if path.exists():
            path.unlink()
            removed.append(rel_path)

    return restored, removed


def parse_args(argv: list = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--create-snapshot",
        action="store_true",
        help="Capture the CURRENT file state as the new reset target. Only do this when everything is healthy.",
    )
    parser.add_argument("--snapshot-dir", type=str, default=str(DEFAULT_SNAPSHOT_DIR))
    return parser.parse_args(argv)


def main(argv: list = None) -> None:
    args = parse_args(argv)
    snapshot_dir = Path(args.snapshot_dir)

    if args.create_snapshot:
        captured = create_snapshot(snapshot_dir)
        print(f"Snapshot captured under {snapshot_dir}/:")
        for path in captured:
            print(f"  {path}")
        return

    restored, removed = reset_demo(snapshot_dir)
    print("Restored to known-good state:")
    for path in restored:
        print(f"  {path}")
    if removed:
        print("Cleared stale run artifacts:")
        for path in removed:
            print(f"  {path}")


if __name__ == "__main__":
    main()
