#!/usr/bin/env bash
set -euo pipefail

QUESTION="What is today's total outstanding loan balance?"

pause() {
  printf "\nPress Enter to continue..."
  read -r
}

clear
echo "SELF-HEALING DATA AGENT — MVP"
echo
python3 - <<'PY'
from src.legacy.diagnostic_tools import TOOL_SPECS as D
from src.legacy.repair_tools import TOOL_SPECS as R
from src.legacy.business_tools import TOOL_SPECS as Q

print("Business Q&A agent:", len(Q), "read-only tools")
print("Diagnosis agent:   ", len(D), "read-only tools")
print("Repair agent:      ", len(R), "read-only planning tools")
print("\nNo arbitrary filesystem, shell, or code-execution access.")
PY

pause
clear
echo "SCENARIO 1 — CLEAN DATA"
python3 -m src.legacy.ask "$QUESTION" \
  --scenario-manifest-file data/processed/repair_manifest.json

pause
clear
echo "SCENARIO 2 — UNAPPROVED BUSINESS VALUE"
python3 -m src.legacy.ask "$QUESTION" \
  --scenario-manifest-file \
  data/scenarios/settled_bug/repair_manifest.json

pause
clear
echo "SCENARIO 3 — INCORRECT ETL JOIN"

python3 - <<'PY'
from pathlib import Path

path = Path("src/legacy/transform.py")
text = path.read_text()

fixed = '''    portfolio = loans_df.merge(
        payments_by_loan.rename("total_paid"), on="loan_id", how="left"
    )
    # Preserve loans with no successful payments; treat missing totals as zero.
    if "total_paid" in portfolio.columns:
        portfolio["total_paid"] = portfolio["total_paid"].fillna(0.0)
    else:
        portfolio["total_paid"] = 0.0'''

buggy = '''    portfolio = loans_df.merge(
        payments_by_loan.rename("total_paid"), on="loan_id", how="inner"
    )'''

if fixed in text:
    path.write_text(text.replace(fixed, buggy))
    print("Injected bug: LEFT JOIN → INNER JOIN")
elif buggy in text:
    print("Bug already present.")
else:
    raise RuntimeError("Expected join implementation was not found.")
PY

echo
echo "Generating failed pipeline state..."
python3 -m src.legacy.run_incorrect_join_pipeline || true

echo
echo "Running diagnosis, repair, verification, and final answer..."
python3 -m src.legacy.ask "$QUESTION" \
  --scenario-manifest-file \
  data/scenarios/incorrect_join/repair_manifest.json

echo
echo "Demo complete."
echo "Run 'python3 -m src.legacy.reset_demo' to restore the clean state."