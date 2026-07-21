"""Independent (pandas, not Spark) validation of the campaign_funnel curated table.

Recomputes every campaign's funnel counts directly from raw Parquet using this module's
own pandas logic, then compares against the curated output row by row. Deliberately does
NOT import src.etl_spark_campaign_funnel -- see that module's docstring for why.
"""

from __future__ import annotations

import pandas as pd

from src.lifecycle_validation_helpers import is_missing
from src.storage import S3Storage

COUNT_COLUMNS = [
    "emails_sent", "emails_opened", "emails_clicked", "offers_created",
    "applications_submitted", "applications_approved", "loans_funded",
]

# (numerator, denominator, output_column) -- mirrors src/etl_spark_campaign_funnel.py's
# RATE_COLUMNS exactly. Counts alone don't catch a bug in the RATE FORMULA itself (e.g. a
# swapped numerator/denominator): the counts feeding a wrong rate formula can still all be
# correct while the derived rate is wrong, so rates need their own independent check.
RATE_COLUMNS = [
    ("emails_opened", "emails_sent", "open_rate"),
    ("emails_clicked", "emails_opened", "click_through_rate"),
    ("offers_created", "emails_clicked", "click_to_offer_rate"),
    ("applications_submitted", "offers_created", "offer_to_application_rate"),
    ("applications_approved", "applications_submitted", "application_to_approval_rate"),
    ("loans_funded", "applications_approved", "approval_to_funded_rate"),
]


def _independent_campaign_funnel(
    campaigns: pd.DataFrame,
    email_events: pd.DataFrame,
    prequal_offers: pd.DataFrame,
    applications: pd.DataFrame,
    underwriting_decisions: pd.DataFrame,
    loans: pd.DataFrame,
) -> pd.DataFrame:
    offer_campaign = prequal_offers[["offer_id", "campaign_id"]].rename(
        columns={"campaign_id": "attributed_campaign_id"}
    )
    app_attribution = applications[["application_id", "offer_id"]].merge(offer_campaign, on="offer_id", how="left")

    campaign_ids = list(campaigns["campaign_id"].unique()) + [None]
    rows = []
    for campaign_id in campaign_ids:
        if campaign_id is None:
            emails = email_events[email_events["campaign_id"].isna()]
            offers = prequal_offers[prequal_offers["campaign_id"].isna()]
        else:
            emails = email_events[email_events["campaign_id"] == campaign_id]
            offers = prequal_offers[prequal_offers["campaign_id"] == campaign_id]

        apps_for_campaign = app_attribution[
            app_attribution["attributed_campaign_id"].isna()
            if campaign_id is None
            else app_attribution["attributed_campaign_id"] == campaign_id
        ]
        app_ids_for_campaign = set(apps_for_campaign["application_id"])

        approved_apps = set(
            underwriting_decisions[underwriting_decisions["decision"] == "APPROVED"]["application_id"]
        )
        applications_approved = len(app_ids_for_campaign & approved_apps)

        loans_funded = int(loans["application_id"].isin(app_ids_for_campaign).sum())

        row = {
            "campaign_id": campaign_id,
            "emails_sent": int((emails["event_type"] == "SENT").sum()),
            "emails_opened": int((emails["event_type"] == "OPENED").sum()),
            "emails_clicked": int((emails["event_type"] == "CLICKED").sum()),
            "offers_created": int(len(offers)),
            "applications_submitted": int(len(apps_for_campaign)),
            "applications_approved": applications_approved,
            "loans_funded": loans_funded,
        }
        for numerator, denominator, output_column in RATE_COLUMNS:
            row[output_column] = round(row[numerator] / row[denominator], 4) if row[denominator] > 0 else None
        rows.append(row)

    return pd.DataFrame(rows)


def _reconciliation_check(rule: dict, tolerance: int, expected: int, actual: int, campaign_id) -> dict:
    difference = int(actual) - int(expected)
    status = "PASS" if abs(difference) <= tolerance else "FAIL"
    return {
        "id": rule["id"],
        "description": rule["description"],
        "status": status,
        "expected": int(expected),
        "actual": int(actual),
        "difference": difference,
        "details": None if status == "PASS" else f"campaign_id={campaign_id!r}",
    }


def validate_campaign_funnel(storage: S3Storage, validation_rules: dict) -> dict:
    campaigns = storage.read_parquet("raw/campaigns.parquet")
    email_events = storage.read_parquet("raw/email_events.parquet")
    prequal_offers = storage.read_parquet("raw/prequal_offers.parquet")
    applications = storage.read_parquet("raw/applications.parquet")
    underwriting_decisions = storage.read_parquet("raw/underwriting_decisions.parquet")
    loans = storage.read_parquet("raw/loans.parquet")
    curated = storage.read_parquet("curated/campaign_funnel.parquet")

    expected = _independent_campaign_funnel(
        campaigns, email_events, prequal_offers, applications, underwriting_decisions, loans
    )

    rules = {rule["id"]: rule for rule in validation_rules["rules"]}
    tolerance = validation_rules["tolerance"]["count"]
    rate_tolerance = validation_rules["tolerance"]["rate"]

    checks = []
    # pandas silently turns a None campaign_id into a float NaN when building a DataFrame
    # from a list of dicts -- and NaN != NaN, so a naive dict key of the raw value would
    # never match between "expected" and "curated" for the organic row. Normalize both
    # sides through pd.notna() so the organic row's key is unambiguously Python None.
    curated_by_campaign = {
        (row["campaign_id"] if pd.notna(row["campaign_id"]) else None): row for _, row in curated.iterrows()
    }
    expected_by_campaign = {
        (row["campaign_id"] if pd.notna(row["campaign_id"]) else None): row for _, row in expected.iterrows()
    }

    missing_campaigns = set(expected_by_campaign) - set(curated_by_campaign)
    for column in COUNT_COLUMNS:
        rule = rules[f"{column}_reconciliation"]
        total_expected = sum(int(row[column]) for row in expected_by_campaign.values())
        total_actual = sum(
            int(curated_by_campaign[cid][column]) for cid in expected_by_campaign if cid in curated_by_campaign
        )
        checks.append(_reconciliation_check(rule, tolerance, total_expected, total_actual, "ALL"))

    def _rate_mismatch(expected_row, actual_row, column) -> bool:
        expected_value, actual_value = expected_row[column], actual_row[column]
        expected_missing, actual_missing = is_missing(expected_value), is_missing(actual_value)
        if expected_missing and actual_missing:
            return False
        if expected_missing != actual_missing:
            return True
        return abs(float(actual_value) - float(expected_value)) > rate_tolerance

    per_campaign_mismatch_rule = rules["campaign_funnel_row_counts_match_per_campaign"]
    mismatched = []
    for campaign_id, expected_row in expected_by_campaign.items():
        actual_row = curated_by_campaign.get(campaign_id)
        if actual_row is None:
            mismatched.append(campaign_id)
            continue
        if any(int(actual_row[c]) != int(expected_row[c]) for c in COUNT_COLUMNS):
            mismatched.append(campaign_id)
        elif any(_rate_mismatch(expected_row, actual_row, c) for _, _, c in RATE_COLUMNS):
            mismatched.append(campaign_id)
    checks.append(
        {
            "id": per_campaign_mismatch_rule["id"],
            "description": per_campaign_mismatch_rule["description"],
            "status": "PASS" if not mismatched and not missing_campaigns else "FAIL",
            "expected": 0,
            "actual": len(mismatched) + len(missing_campaigns),
            "difference": len(mismatched) + len(missing_campaigns),
            "details": f"mismatched or missing campaign_ids: {mismatched + list(missing_campaigns)}"
            if (mismatched or missing_campaigns)
            else None,
        }
    )

    failed = [c for c in checks if c["status"] == "FAIL"]
    return {
        "overall_status": "PASS" if not failed else "FAIL",
        "total_check_count": len(checks),
        "failed_check_count": len(failed),
        "checks": checks,
    }


def main(argv: list[str] | None = None) -> None:
    storage = S3Storage()
    validation_rules = storage.read_json("context/validations/campaign_funnel.json")
    results = validate_campaign_funnel(storage, validation_rules)
    storage.write_json("curated/campaign_funnel_validation_results.json", results)

    print(f"overall_status: {results['overall_status']}")
    for check in results["checks"]:
        marker = "OK  " if check["status"] == "PASS" else "FAIL"
        print(f"  [{marker}] {check['id']}  expected={check['expected']} actual={check['actual']}")

    if results["overall_status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
