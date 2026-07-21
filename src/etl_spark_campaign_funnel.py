"""PySpark ETL: campaign_funnel -- one curated row per campaign, plus one campaign_id=NULL
"organic" row for engagement/applications/loans not attributable to any campaign.

Reads s3a://<bucket>/raw/{campaigns,email_events,prequal_offers,applications,
underwriting_decisions,loans}.parquet, writes to s3a://<bucket>/curated/campaign_funnel.parquet.

See src/validate_campaign_funnel.py for the independent (pandas) recomputation used to
check this ETL's output -- never import one from the other.
"""

from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.spark_session import get_spark_session, s3a_path
from src.storage import S3Storage

CURATED_KEY = "curated/campaign_funnel.parquet"

# Spark's equi-join treats NULL != NULL (standard SQL semantics) -- a plain
# `.join(other, on="campaign_id")` would silently DROP the organic (campaign_id=NULL)
# row's data from every join below, since NULL never matches NULL. Coalescing to this
# sentinel before joining, then back to NULL in the final result, sidesteps that
# entirely rather than relying on eqNullSafe at every join site.
_ORGANIC_SENTINEL = "__ORGANIC__"


def _sentinel_for_null(column):
    return F.coalesce(F.col(column), F.lit(_ORGANIC_SENTINEL))

RATE_COLUMNS = [
    # (numerator, denominator, output_column)
    ("emails_opened", "emails_sent", "open_rate"),
    ("emails_clicked", "emails_opened", "click_through_rate"),
    ("offers_created", "emails_clicked", "click_to_offer_rate"),
    ("applications_submitted", "offers_created", "offer_to_application_rate"),
    ("applications_approved", "applications_submitted", "application_to_approval_rate"),
    ("loans_funded", "applications_approved", "approval_to_funded_rate"),
]

COUNT_COLUMNS = [
    "emails_sent", "emails_opened", "emails_clicked", "offers_created",
    "applications_submitted", "applications_approved", "loans_funded",
]


def _attribute_applications_to_campaigns(applications: DataFrame, prequal_offers: DataFrame) -> DataFrame:
    """Every application attributed to a campaign_id via its offer -- organic offer or
    organic (no-offer) application both attribute to the organic sentinel.
    """
    offer_campaign = prequal_offers.select(
        F.col("offer_id"), _sentinel_for_null("campaign_id").alias("attributed_campaign_id")
    )
    applications_with_sentinel_offer_id = applications.withColumn(
        "offer_id", F.coalesce(F.col("offer_id"), F.lit("__NO_OFFER__"))
    )
    offer_campaign_with_sentinel_offer_id = offer_campaign.withColumnRenamed("offer_id", "offer_id_key")
    joined = applications_with_sentinel_offer_id.join(
        offer_campaign_with_sentinel_offer_id,
        applications_with_sentinel_offer_id["offer_id"] == offer_campaign_with_sentinel_offer_id["offer_id_key"],
        how="left",
    )
    # A truly organic application (no offer at all) has no match above -- also organic.
    return joined.select(
        "application_id", F.coalesce(F.col("attributed_campaign_id"), F.lit(_ORGANIC_SENTINEL)).alias("attributed_campaign_id")
    )


def compute_campaign_funnel(spark: SparkSession) -> DataFrame:
    campaigns = spark.read.parquet(s3a_path("raw", "campaigns.parquet"))
    email_events = spark.read.parquet(s3a_path("raw", "email_events.parquet"))
    prequal_offers = spark.read.parquet(s3a_path("raw", "prequal_offers.parquet"))
    applications = spark.read.parquet(s3a_path("raw", "applications.parquet"))
    underwriting_decisions = spark.read.parquet(s3a_path("raw", "underwriting_decisions.parquet"))
    loans = spark.read.parquet(s3a_path("raw", "loans.parquet"))

    # The sentinel substitution below silently merges a real campaign_id into the organic
    # bucket if it ever happens to equal the sentinel string -- guard against that
    # (malformed upstream data, a colliding test fixture, a future naming scheme) rather
    # than let it fail silently the exact same way the NULL-join bug this sentinel fixes
    # originally did.
    if campaigns.filter(F.col("campaign_id") == _ORGANIC_SENTINEL).limit(1).count() > 0:
        raise ValueError(f"a real campaign_id collides with the reserved organic sentinel {_ORGANIC_SENTINEL!r}")

    app_attribution = _attribute_applications_to_campaigns(applications, prequal_offers)

    emails_by_campaign = email_events.withColumn("campaign_key", _sentinel_for_null("campaign_id")).groupBy(
        "campaign_key"
    ).agg(
        F.sum(F.when(F.col("event_type") == "SENT", 1).otherwise(0)).alias("emails_sent"),
        F.sum(F.when(F.col("event_type") == "OPENED", 1).otherwise(0)).alias("emails_opened"),
        F.sum(F.when(F.col("event_type") == "CLICKED", 1).otherwise(0)).alias("emails_clicked"),
    )

    offers_by_campaign = (
        prequal_offers.withColumn("campaign_key", _sentinel_for_null("campaign_id"))
        .groupBy("campaign_key")
        .agg(F.count(F.lit(1)).alias("offers_created"))
    )

    applications_by_campaign = app_attribution.groupBy(
        F.col("attributed_campaign_id").alias("campaign_key")
    ).agg(F.count(F.lit(1)).alias("applications_submitted"))

    approved_by_campaign = (
        underwriting_decisions.filter(F.col("decision") == "APPROVED")
        .join(app_attribution, on="application_id", how="inner")
        .groupBy(F.col("attributed_campaign_id").alias("campaign_key"))
        .agg(F.count(F.lit(1)).alias("applications_approved"))
    )

    loans_by_campaign = (
        loans.join(app_attribution, on="application_id", how="inner")
        .groupBy(F.col("attributed_campaign_id").alias("campaign_key"))
        .agg(F.count(F.lit(1)).alias("loans_funded"))
    )

    organic_row = spark.createDataFrame([(_ORGANIC_SENTINEL, None, None)], "campaign_key string, name string, channel string")
    universe = (
        campaigns.select(F.col("campaign_id").alias("campaign_key"), "name", "channel")
        .unionByName(organic_row)
    )

    result = (
        universe.join(emails_by_campaign, on="campaign_key", how="left")
        .join(offers_by_campaign, on="campaign_key", how="left")
        .join(applications_by_campaign, on="campaign_key", how="left")
        .join(approved_by_campaign, on="campaign_key", how="left")
        .join(loans_by_campaign, on="campaign_key", how="left")
        .fillna(0, subset=COUNT_COLUMNS)
    )

    result = result.withColumn(
        "campaign_id",
        F.when(F.col("campaign_key") == _ORGANIC_SENTINEL, F.lit(None).cast("string")).otherwise(F.col("campaign_key")),
    ).drop("campaign_key")

    for numerator, denominator, output_column in RATE_COLUMNS:
        result = result.withColumn(
            output_column,
            F.when(F.col(denominator) > 0, F.round(F.col(numerator) / F.col(denominator), 4)),
        )

    return result


def write_curated(df: DataFrame, storage: S3Storage) -> str:
    """Collect (this is a small, one-row-per-campaign table) and write as one clean
    Parquet object -- see src/etl_spark_loan_portfolio.py's write_curated for why.
    """
    storage.write_parquet(CURATED_KEY, df.toPandas())
    return f"s3://{storage.bucket}/{CURATED_KEY}"


def main() -> None:
    storage = S3Storage()
    spark = get_spark_session("campaign-funnel-etl")
    spark.sparkContext.setLogLevel("WARN")
    try:
        result_df = compute_campaign_funnel(spark)
        pandas_df = result_df.toPandas()
        storage.write_parquet(CURATED_KEY, pandas_df)
        path = f"s3://{storage.bucket}/{CURATED_KEY}"
        sorted_df = pandas_df.sort_values("campaign_id", na_position="last")
        print(f"Wrote {path} ({len(sorted_df)} rows)")
        for d in sorted_df.to_dict(orient="records"):
            # pandas represents a null campaign_id as NaN (a truthy float, unlike Python's
            # None), so `d["campaign_id"] or "ORGANIC"` alone would print "nan" here.
            label = "ORGANIC" if pd.isna(d["campaign_id"]) else d["campaign_id"]
            print(f"  {label:<10} sent={d['emails_sent']:<5} loans_funded={d['loans_funded']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
