"""PySpark ETL: coupon_performance -- one curated row per coupon_code, measuring how many
prequalification offers, submitted applications, and funded loans trace back to it.

Reads s3a://<bucket>/raw/{coupon_rules,prequal_offers,applications,loans}.parquet, writes to
s3a://<bucket>/curated/coupon_performance.parquet.

coupon_code is NOT unique across coupon_rules (see generate_data.py's
generate_coupon_rules docstring: "coupon_code is drawn from a small reusable pool -- it is NOT
unique across coupon_rules; coupon_rule_id is the real primary key"), and prequal_offers only
records the CODE an offer used, never which specific coupon_rule_id -- so a code with more
than one underlying rule is a genuine, structural ambiguity, not a bug in this ETL.
coupon_rule_count/currently_valid_rule_count report that ambiguity directly instead of picking
one rule's discount_type/discount_value to display and silently hiding the rest.

See src/validate_coupon_performance.py for the independent (pandas) recomputation used to
check this ETL's output -- never import one from the other.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.spark_session import get_spark_session, s3a_path
from src.storage import S3Storage

CURATED_KEY = "curated/coupon_performance.parquet"
DEFAULT_AS_OF_DATE = "2026-07-20"

COUNT_COLUMNS = ["coupon_rule_count", "currently_valid_rule_count", "offers_created", "applications_submitted", "loans_funded"]


def compute_coupon_performance(spark: SparkSession, business_rules: dict, as_of_date: str = DEFAULT_AS_OF_DATE) -> DataFrame:
    """business_rules is accepted (unused) to match the standardized generic-pipeline ETL
    signature (spark, business_rules, as_of_date) -- see pipelines/coupon_performance.yaml and
    src.manifest_loader.build_generic_pipeline_spec -- so this pipeline needs no hand-written
    registry adapter, only a manifest."""
    coupon_rules = spark.read.parquet(s3a_path("raw", "coupon_rules.parquet"))
    prequal_offers = spark.read.parquet(s3a_path("raw", "prequal_offers.parquet"))
    applications = spark.read.parquet(s3a_path("raw", "applications.parquet"))
    loans = spark.read.parquet(s3a_path("raw", "loans.parquet"))

    as_of_ts = F.lit(as_of_date).cast("date")

    rules_by_code = coupon_rules.groupBy("coupon_code").agg(
        F.countDistinct("coupon_rule_id").alias("coupon_rule_count"),
        F.sum(
            F.when(
                (F.col("valid_from").cast("date") <= as_of_ts) & (F.col("valid_to").cast("date") >= as_of_ts), 1
            ).otherwise(0)
        ).alias("currently_valid_rule_count"),
    )

    # A point-in-time snapshot -- an offer created after as_of_date hasn't "happened yet" as
    # of this run, same discipline as src/etl_spark_loan_portfolio.py's accrued-interest cutoff.
    offers_as_of = prequal_offers.filter(F.to_date(F.col("created_at")) <= as_of_ts)
    coupon_offers = offers_as_of.filter(F.col("coupon_code").isNotNull())

    offers_by_code = coupon_offers.groupBy("coupon_code").agg(F.count(F.lit(1)).alias("offers_created"))

    # Attribute an application to a coupon_code via its originating offer (inner join --
    # applications with no offer, or whose offer used no coupon, contribute to no code here).
    app_coupon = applications.join(
        coupon_offers.select("offer_id", "coupon_code"), on="offer_id", how="inner"
    )
    applications_by_code = app_coupon.groupBy("coupon_code").agg(F.count(F.lit(1)).alias("applications_submitted"))

    loans_by_code = (
        loans.join(app_coupon.select("application_id", "coupon_code"), on="application_id", how="inner")
        .groupBy("coupon_code")
        .agg(F.count(F.lit(1)).alias("loans_funded"))
    )

    # The catalog is every code that was ever DEFINED, whether or not it was ever used --
    # a coupon with zero redemptions is a real, reportable fact, not a row to drop.
    result = (
        rules_by_code.join(offers_by_code, on="coupon_code", how="left")
        .join(applications_by_code, on="coupon_code", how="left")
        .join(loans_by_code, on="coupon_code", how="left")
        .fillna(0, subset=COUNT_COLUMNS)
    )

    result = result.withColumn(
        "redemption_rate",
        F.when(F.col("offers_created") > 0, F.round(F.col("loans_funded") / F.col("offers_created"), 4)),
    ).withColumn("as_of_date", F.lit(as_of_date))

    return result


def write_curated(df: DataFrame, storage: S3Storage) -> str:
    storage.write_parquet(CURATED_KEY, df.toPandas())
    return f"s3://{storage.bucket}/{CURATED_KEY}"


def main() -> None:
    storage = S3Storage()
    business_rules = storage.read_json("context/business_rules.json")
    spark = get_spark_session("coupon-performance-etl")
    spark.sparkContext.setLogLevel("WARN")
    try:
        result_df = compute_coupon_performance(spark, business_rules, DEFAULT_AS_OF_DATE)
        pandas_df = result_df.toPandas()
        storage.write_parquet(CURATED_KEY, pandas_df)
        path = f"s3://{storage.bucket}/{CURATED_KEY}"
        print(f"Wrote {path} ({len(pandas_df)} rows)")
        for d in pandas_df.sort_values("coupon_code").to_dict(orient="records"):
            print(f"  {d['coupon_code']:<12} offers={d['offers_created']:<4} loans_funded={d['loans_funded']:<3} redemption_rate={d['redemption_rate']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
