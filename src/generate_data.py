"""Deterministic synthetic data generator for the data-agent MVP.

Generates a coherent, full-lifecycle banking dataset for a simulated lending
company: customers, marketing campaigns/coupons/email engagement, prequalified
offers, applications, underwriting decisions, funded loans, payment schedules,
actual payment events, delinquency snapshots, and defaults. All randomness
flows through a single seeded random.Random instance, and all dates are
computed relative to a fixed --as-of-date rather than the machine clock, so a
given (seed, num-customers, as-of-date) always produces byte-for-byte
identical output.

The lifecycle is generated forward, stage by stage, each stage a
probabilistic subset of the previous one (customer -> campaign engagement ->
offer -> application -> underwriting decision -> funded loan -> scheduled
payments -> actual payment events -> delinquency/default), so funnel counts
only shrink moving downstream. Loan status (ACTIVE/CLOSED/DEFAULTED) is
derived from how much of the loan's term has elapsed by as_of_date, not
assigned independently of time -- a loan whose full term has already elapsed
is realistically CLOSED or DEFAULTED, one still within its term is ACTIVE
(or, rarely, an early default).

This module intentionally does not implement ETL, validation, agent, or
repair logic -- see README.md for scope.
"""

from __future__ import annotations

import argparse
import calendar
import json
import random
from collections import Counter
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

from src.schemas import (
    Application,
    ApplicationStatus,
    Campaign,
    Channel,
    CouponRule,
    CreditScoreBand,
    Customer,
    Decision,
    Default,
    DelinquencyBucket,
    DelinquencyEvent,
    DiscountType,
    EmailEvent,
    EmailEventType,
    IncomeBand,
    Loan,
    LoanStatus,
    PaymentEvent,
    PaymentEventStatus,
    PaymentEventType,
    PaymentMethod,
    PaymentScheduleEntry,
    PrequalOffer,
    RejectionReason,
    RiskSegment,
    UnderwritingDecision,
    US_STATE_CODES,
)

DEFAULT_SEED = 42
DEFAULT_NUM_CUSTOMERS = 100
DEFAULT_OUTPUT_DIR = "data/raw"
DEFAULT_AS_OF_DATE = "2026-07-20"

CUSTOMER_HISTORY_DAYS = 3 * 365

# Days of runway every "first touch" stage (organic offer/application, or a
# campaign's own start date) must leave before as_of_date, so the remaining
# small per-stage delays (all 0-10 days) always land on or before as_of_date.
FUNNEL_TAIL_BUFFER_DAYS = 45

RISK_SEGMENT_WEIGHTS: dict[RiskSegment, float] = {
    RiskSegment.LOW: 0.40,
    RiskSegment.MEDIUM: 0.35,
    RiskSegment.HIGH: 0.25,
}

CREDIT_BAND_WEIGHTS_BY_RISK: dict[RiskSegment, dict[CreditScoreBand, float]] = {
    RiskSegment.LOW: {
        CreditScoreBand.UNDER_620: 0.02,
        CreditScoreBand.RANGE_620_679: 0.08,
        CreditScoreBand.RANGE_680_719: 0.20,
        CreditScoreBand.RANGE_720_759: 0.35,
        CreditScoreBand.PLUS_760: 0.35,
    },
    RiskSegment.MEDIUM: {
        CreditScoreBand.UNDER_620: 0.10,
        CreditScoreBand.RANGE_620_679: 0.25,
        CreditScoreBand.RANGE_680_719: 0.30,
        CreditScoreBand.RANGE_720_759: 0.25,
        CreditScoreBand.PLUS_760: 0.10,
    },
    RiskSegment.HIGH: {
        CreditScoreBand.UNDER_620: 0.40,
        CreditScoreBand.RANGE_620_679: 0.30,
        CreditScoreBand.RANGE_680_719: 0.15,
        CreditScoreBand.RANGE_720_759: 0.10,
        CreditScoreBand.PLUS_760: 0.05,
    },
}

# Raw numeric FICO-style score underlying each band -- kept alongside the band
# (not instead of it) so a future schema-drift scenario (e.g. this field
# changing from int to string) has a real value to corrupt.
CREDIT_SCORE_RANGE_BY_BAND: dict[CreditScoreBand, tuple[int, int]] = {
    CreditScoreBand.UNDER_620: (300, 619),
    CreditScoreBand.RANGE_620_679: (620, 679),
    CreditScoreBand.RANGE_680_719: (680, 719),
    CreditScoreBand.RANGE_720_759: (720, 759),
    CreditScoreBand.PLUS_760: (760, 850),
}

INCOME_BAND_WEIGHTS: dict[IncomeBand, float] = {
    IncomeBand.UNDER_40000: 0.15,
    IncomeBand.RANGE_40000_60000: 0.25,
    IncomeBand.RANGE_60000_80000: 0.25,
    IncomeBand.RANGE_80000_120000: 0.20,
    IncomeBand.OVER_120000: 0.15,
}

# --- Campaigns / coupons ---

CAMPAIGNS_PER_100_CUSTOMERS = 8
CAMPAIGN_CHANNEL_WEIGHTS: dict[Channel, float] = {
    Channel.EMAIL: 0.60,
    Channel.SOCIAL: 0.25,
    Channel.PARTNER: 0.15,
}
CAMPAIGN_DURATION_DAYS_RANGE = (30, 90)
CAMPAIGN_NAME_TEMPLATES = (
    "Spring Save",
    "Summer Cashback",
    "Referral Boost",
    "Holiday Rates",
    "New Year Fresh Start",
    "Back to School",
    "Partner Rewards",
    "Loyalty Bonus",
)

CAMPAIGN_HAS_COUPON_PROBABILITY = 0.5
# Deliberately a small, reusable pool: the same code string can be attached to
# more than one coupon_rule/campaign over time, the way marketing teams reuse
# code names -- coupon_rule_id (not coupon_code) is the real primary key.
COUPON_CODE_POOL = ("WELCOME10", "SPRING5", "LOYALTY15", "REFER20", "HOLIDAY10", "FLASH25")
COUPON_DISCOUNT_TYPE_WEIGHTS: dict[DiscountType, float] = {
    DiscountType.RATE_DISCOUNT: 0.7,
    DiscountType.FEE_WAIVER: 0.3,
}
COUPON_DISCOUNT_VALUE_RANGE_BY_TYPE: dict[DiscountType, tuple[float, float]] = {
    DiscountType.RATE_DISCOUNT: (0.01, 0.03),
    DiscountType.FEE_WAIVER: (25.0, 100.0),
}

# --- Email engagement funnel ---

EMAIL_TARGET_RATE = 0.65
EMAIL_OPEN_RATE = 0.55
EMAIL_CLICK_RATE = 0.45
EMAIL_OPEN_DELAY_DAYS = (0, 5)
EMAIL_CLICK_DELAY_DAYS = (0, 3)

# --- Prequalified offers ---

OFFER_RATE_GIVEN_CLICK = 0.75
ORGANIC_OFFER_RATE = 0.10  # customers with no campaign engagement can still get an organic offer
OFFER_CREATION_DELAY_DAYS = (0, 5)
OFFER_VALIDITY_DAYS = 30
OFFER_COUPON_ATTACH_RATE = 0.5

# --- Applications ---

APPLICATION_RATE_GIVEN_OFFER = 0.70
ORGANIC_APPLICATION_RATE = 0.08  # applying directly, with no prior offer
APPLICATION_SUBMIT_DELAY_DAYS = (0, 5)
APPLICATION_STATUS_WEIGHTS: dict[ApplicationStatus, float] = {
    ApplicationStatus.DECISIONED: 0.90,
    ApplicationStatus.WITHDRAWN: 0.07,
    ApplicationStatus.SUBMITTED: 0.03,  # still pending as of as_of_date -- no decision generated
}

# --- Underwriting decisions ---

DECISION_WEIGHTS_BY_RISK: dict[RiskSegment, dict[Decision, float]] = {
    RiskSegment.LOW: {Decision.APPROVED: 0.85, Decision.MANUAL_REVIEW: 0.08, Decision.REJECTED: 0.07},
    RiskSegment.MEDIUM: {Decision.APPROVED: 0.65, Decision.MANUAL_REVIEW: 0.12, Decision.REJECTED: 0.23},
    RiskSegment.HIGH: {Decision.APPROVED: 0.40, Decision.MANUAL_REVIEW: 0.12, Decision.REJECTED: 0.48},
}
REJECTION_REASON_WEIGHTS: dict[RejectionReason, float] = {
    RejectionReason.LOW_CREDIT_SCORE: 0.35,
    RejectionReason.HIGH_DEBT_TO_INCOME: 0.25,
    RejectionReason.INSUFFICIENT_INCOME: 0.20,
    RejectionReason.INCOMPLETE_APPLICATION: 0.10,
    RejectionReason.FRAUD_RISK: 0.10,
}
DECISION_DELAY_DAYS = (0, 5)
APPROVED_AMOUNT_FACTOR_RANGE = (0.8, 1.0)
# Decisions before this many days back use the older underwriting model --
# deterministic from decided_at, not random, so "did approval rates change
# after the new model" is a real, answerable question later.
MODEL_CUTOVER_DAYS_BEFORE_AS_OF = 180

# --- Loans (derived from decisions, not issued independently) ---

TERM_MONTHS_WEIGHTS: dict[int, float] = {
    12: 0.65,
    24: 0.20,
    36: 0.09,
    48: 0.04,
    60: 0.02,
}
INTEREST_RATE_RANGE_BY_RISK: dict[RiskSegment, tuple[float, float]] = {
    RiskSegment.LOW: (0.04, 0.09),
    RiskSegment.MEDIUM: (0.08, 0.15),
    RiskSegment.HIGH: (0.14, 0.25),
}
PRINCIPAL_AMOUNT_RANGE = (2000.0, 40000.0)

FUNDED_RATE_GIVEN_APPROVED = 0.90
LOAN_FUNDING_DELAY_DAYS = (1, 10)
LOAN_STATUS_EARLY_DEFAULT_PROBABILITY_BY_RISK: dict[RiskSegment, float] = {
    RiskSegment.LOW: 0.02,
    RiskSegment.MEDIUM: 0.05,
    RiskSegment.HIGH: 0.12,
}
# Once a loan's full term has elapsed by as_of_date, it's realistically
# CLOSED (repaid) or DEFAULTED -- never still ACTIVE.
LOAN_STATUS_WEIGHTS_AT_TERM_END_BY_RISK: dict[RiskSegment, dict[LoanStatus, float]] = {
    RiskSegment.LOW: {LoanStatus.CLOSED: 0.92, LoanStatus.DEFAULTED: 0.08},
    RiskSegment.MEDIUM: {LoanStatus.CLOSED: 0.85, LoanStatus.DEFAULTED: 0.15},
    RiskSegment.HIGH: {LoanStatus.CLOSED: 0.70, LoanStatus.DEFAULTED: 0.30},
}

# --- Payment schedule / payment events ---

PAYMENT_METHOD_WEIGHTS: dict[PaymentMethod, float] = {
    PaymentMethod.ACH: 0.60,
    PaymentMethod.CARD: 0.30,
    PaymentMethod.CHECK: 0.10,
}
PAYMENT_EVENT_OUTCOME_WEIGHTS_BY_RISK: dict[RiskSegment, dict[PaymentEventStatus, float]] = {
    RiskSegment.LOW: {
        PaymentEventStatus.PAID: 0.90,
        PaymentEventStatus.LATE: 0.07,
        PaymentEventStatus.MISSED: 0.02,
        PaymentEventStatus.FAILED: 0.01,
    },
    RiskSegment.MEDIUM: {
        PaymentEventStatus.PAID: 0.80,
        PaymentEventStatus.LATE: 0.12,
        PaymentEventStatus.MISSED: 0.05,
        PaymentEventStatus.FAILED: 0.03,
    },
    RiskSegment.HIGH: {
        PaymentEventStatus.PAID: 0.65,
        PaymentEventStatus.LATE: 0.20,
        PaymentEventStatus.MISSED: 0.10,
        PaymentEventStatus.FAILED: 0.05,
    },
}

# Recent-history/future windows for loans that don't need a full schedule realized.
ACTIVE_PAST_WINDOW = 3
ACTIVE_FUTURE_WINDOW = 3
DEFAULTED_HISTORY_WINDOW = 4

REVERSAL_RATE_GIVEN_PAID = 0.03
REVERSAL_DELAY_DAYS = (1, 10)

# --- Delinquency / default ---

DELINQUENCY_BUCKET_BY_STATUS: dict[PaymentEventStatus, DelinquencyBucket] = {
    PaymentEventStatus.LATE: DelinquencyBucket.DAYS_30,
    PaymentEventStatus.MISSED: DelinquencyBucket.DAYS_60,
}
DELINQUENCY_DAYS_PAST_DUE_RANGE_BY_BUCKET: dict[DelinquencyBucket, tuple[int, int]] = {
    DelinquencyBucket.DAYS_30: (1, 29),
    DelinquencyBucket.DAYS_60: (30, 59),
    DelinquencyBucket.DAYS_90_PLUS: (90, 150),
}

RECOVERY_PROBABILITY = 0.15
RECOVERY_FACTOR_RANGE = (0.1, 0.4)
RECOVERY_DELAY_DAYS = (10, 60)

TABLE_ID_FIELDS: dict[str, str] = {
    "customers": "customer_id",
    "campaigns": "campaign_id",
    "coupon_rules": "coupon_rule_id",
    "email_events": "event_id",
    "prequal_offers": "offer_id",
    "applications": "application_id",
    "underwriting_decisions": "decision_id",
    "loans": "loan_id",
    "payment_schedule": "schedule_id",
    "payment_events": "event_id",
    "delinquency_events": "delinquency_id",
    "defaults": "default_id",
}


def weighted_choice(rng: random.Random, weights: dict):
    """Pick one key from a {option: weight} mapping using the given rng."""
    options = list(weights.keys())
    probabilities = list(weights.values())
    return rng.choices(options, weights=probabilities, k=1)[0]


def add_months(base: date, months: int) -> date:
    """Return base shifted by a (possibly negative) number of months."""
    month_index = base.month - 1 + months
    year = base.year + month_index // 12
    month = month_index % 12 + 1
    day = min(base.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _months_between(start: date, end: date) -> int:
    """Whole calendar months elapsed from start to end (never negative)."""
    months = (end.year - start.year) * 12 + (end.month - start.month)
    if end.day < start.day:
        months -= 1
    return max(0, months)


def _random_date_between(rng: random.Random, start: date, end: date) -> date:
    span_days = (end - start).days
    if span_days <= 0:
        return start
    return start + timedelta(days=rng.randint(0, span_days))


class _IdGenerator:
    """Sequential zero-padded ID generator, used so IDs are assigned in
    generation order (which is also final sorted order for same-width IDs).
    """

    def __init__(self, prefix: str, width: int) -> None:
        self._prefix = prefix
        self._width = width
        self._counter = 1

    def next_id(self) -> str:
        value = f"{self._prefix}{self._counter:0{self._width}d}"
        self._counter += 1
        return value


def generate_customers(rng: random.Random, num_customers: int, as_of_date: date) -> list[Customer]:
    """Generate `num_customers` customers with dates anchored to as_of_date."""
    if num_customers <= 0:
        raise ValueError("num_customers must be positive")

    earliest_created_at = as_of_date - timedelta(days=CUSTOMER_HISTORY_DAYS)
    customers = []
    for i in range(1, num_customers + 1):
        risk_segment = weighted_choice(rng, RISK_SEGMENT_WEIGHTS)
        credit_score_band = weighted_choice(rng, CREDIT_BAND_WEIGHTS_BY_RISK[risk_segment])
        credit_score = rng.randint(*CREDIT_SCORE_RANGE_BY_BAND[credit_score_band])
        income_band = weighted_choice(rng, INCOME_BAND_WEIGHTS)
        created_at = _random_date_between(rng, earliest_created_at, as_of_date)
        customers.append(
            Customer(
                customer_id=f"C{i:06d}",
                created_at=created_at.isoformat(),
                state=rng.choice(US_STATE_CODES),
                income_band=income_band,
                credit_score_band=credit_score_band,
                credit_score=credit_score,
                risk_segment=risk_segment,
            )
        )
    return customers


def generate_campaigns(
    rng: random.Random, as_of_date: date, num_customers: int, earliest_created_at: date
) -> list[Campaign]:
    """Generate a small set of marketing campaigns spanning the customer history window."""
    num_campaigns = max(1, round(num_customers / 100 * CAMPAIGNS_PER_100_CUSTOMERS))
    latest_start = as_of_date - timedelta(days=CAMPAIGN_DURATION_DAYS_RANGE[1] + FUNNEL_TAIL_BUFFER_DAYS)
    if latest_start < earliest_created_at:
        latest_start = earliest_created_at

    campaigns = []
    for i in range(1, num_campaigns + 1):
        duration = rng.randint(*CAMPAIGN_DURATION_DAYS_RANGE)
        start_date = _random_date_between(rng, earliest_created_at, latest_start)
        end_date = start_date + timedelta(days=duration)
        channel = weighted_choice(rng, CAMPAIGN_CHANNEL_WEIGHTS)
        target_risk_segment = rng.choice([None, *list(RiskSegment)])
        campaigns.append(
            Campaign(
                campaign_id=f"CMP{i:04d}",
                name=f"{rng.choice(CAMPAIGN_NAME_TEMPLATES)} {start_date.year}",
                channel=channel,
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                target_risk_segment=target_risk_segment,
            )
        )
    return campaigns


def generate_coupon_rules(rng: random.Random, campaigns: list[Campaign]) -> list[CouponRule]:
    """Some campaigns get a coupon rule. coupon_code is drawn from a small reusable pool --
    it is NOT unique across coupon_rules; coupon_rule_id is the real primary key.
    """
    coupon_rules = []
    counter = 1
    for campaign in campaigns:
        if rng.random() >= CAMPAIGN_HAS_COUPON_PROBABILITY:
            continue
        discount_type = weighted_choice(rng, COUPON_DISCOUNT_TYPE_WEIGHTS)
        low, high = COUPON_DISCOUNT_VALUE_RANGE_BY_TYPE[discount_type]
        coupon_rules.append(
            CouponRule(
                coupon_rule_id=f"CPN{counter:05d}",
                coupon_code=rng.choice(COUPON_CODE_POOL),
                campaign_id=campaign.campaign_id,
                discount_type=discount_type,
                discount_value=round(rng.uniform(low, high), 4),
                valid_from=campaign.start_date,
                valid_to=campaign.end_date,
            )
        )
        counter += 1
    return coupon_rules


def generate_email_events(
    rng: random.Random, customers: list[Customer], campaigns: list[Campaign], as_of_date: date
) -> tuple[list[EmailEvent], dict[tuple[str, str], date]]:
    """Generate the SENT -> OPENED -> CLICKED funnel per (customer, campaign) pair.

    Returns the events plus a {(customer_id, campaign_id): clicked_date} map so
    downstream offer generation knows who engaged and when.
    """
    id_gen = _IdGenerator("EM", 8)
    email_events: list[EmailEvent] = []
    clicked_at: dict[tuple[str, str], date] = {}

    for campaign in campaigns:
        campaign_start = date.fromisoformat(campaign.start_date)
        campaign_end = date.fromisoformat(campaign.end_date)
        for customer in customers:
            customer_created = date.fromisoformat(customer.created_at)
            window_start = max(campaign_start, customer_created)
            if window_start > campaign_end:
                continue  # customer didn't exist yet during this campaign
            if rng.random() >= EMAIL_TARGET_RATE:
                continue

            sent_date = _random_date_between(rng, window_start, campaign_end)
            email_events.append(
                EmailEvent(
                    event_id=id_gen.next_id(),
                    campaign_id=campaign.campaign_id,
                    customer_id=customer.customer_id,
                    event_type=EmailEventType.SENT,
                    event_timestamp=sent_date.isoformat(),
                )
            )
            if rng.random() >= EMAIL_OPEN_RATE:
                continue

            opened_date = sent_date + timedelta(days=rng.randint(*EMAIL_OPEN_DELAY_DAYS))
            email_events.append(
                EmailEvent(
                    event_id=id_gen.next_id(),
                    campaign_id=campaign.campaign_id,
                    customer_id=customer.customer_id,
                    event_type=EmailEventType.OPENED,
                    event_timestamp=opened_date.isoformat(),
                )
            )
            if rng.random() >= EMAIL_CLICK_RATE:
                continue

            clicked_date = opened_date + timedelta(days=rng.randint(*EMAIL_CLICK_DELAY_DAYS))
            email_events.append(
                EmailEvent(
                    event_id=id_gen.next_id(),
                    campaign_id=campaign.campaign_id,
                    customer_id=customer.customer_id,
                    event_type=EmailEventType.CLICKED,
                    event_timestamp=clicked_date.isoformat(),
                )
            )
            clicked_at[(customer.customer_id, campaign.campaign_id)] = clicked_date

    return email_events, clicked_at


def generate_prequal_offers(
    rng: random.Random,
    customers_by_id: dict[str, Customer],
    clicked_at: dict[tuple[str, str], date],
    coupon_rules_by_campaign: dict[str, list[CouponRule]],
    as_of_date: date,
) -> list[PrequalOffer]:
    """Offers mostly follow a click; a smaller "organic" share go to customers
    with no campaign engagement at all.
    """
    id_gen = _IdGenerator("OFF", 7)
    offers: list[PrequalOffer] = []
    engaged_customer_ids: set[str] = set()

    for (customer_id, campaign_id), clicked_date in clicked_at.items():
        if rng.random() >= OFFER_RATE_GIVEN_CLICK:
            continue
        customer = customers_by_id[customer_id]
        created_date = clicked_date + timedelta(days=rng.randint(*OFFER_CREATION_DELAY_DAYS))
        coupon_code = None
        campaign_coupons = coupon_rules_by_campaign.get(campaign_id, [])
        if campaign_coupons and rng.random() < OFFER_COUPON_ATTACH_RATE:
            coupon_code = rng.choice(campaign_coupons).coupon_code
        rate_low, rate_high = INTEREST_RATE_RANGE_BY_RISK[customer.risk_segment]
        offers.append(
            PrequalOffer(
                offer_id=id_gen.next_id(),
                customer_id=customer_id,
                campaign_id=campaign_id,
                coupon_code=coupon_code,
                offer_amount=round(rng.uniform(*PRINCIPAL_AMOUNT_RANGE), 2),
                offer_apr=round(rng.uniform(rate_low, rate_high), 4),
                created_at=created_date.isoformat(),
                expires_at=(created_date + timedelta(days=OFFER_VALIDITY_DAYS)).isoformat(),
            )
        )
        engaged_customer_ids.add(customer_id)

    latest_offer_date = as_of_date - timedelta(days=FUNNEL_TAIL_BUFFER_DAYS)
    for customer in customers_by_id.values():
        if customer.customer_id in engaged_customer_ids:
            continue
        if rng.random() >= ORGANIC_OFFER_RATE:
            continue
        customer_created = date.fromisoformat(customer.created_at)
        if customer_created > latest_offer_date:
            continue  # too new to have had time to reach an offer before as_of_date
        created_date = _random_date_between(rng, customer_created, latest_offer_date)
        rate_low, rate_high = INTEREST_RATE_RANGE_BY_RISK[customer.risk_segment]
        offers.append(
            PrequalOffer(
                offer_id=id_gen.next_id(),
                customer_id=customer.customer_id,
                campaign_id=None,
                coupon_code=None,
                offer_amount=round(rng.uniform(*PRINCIPAL_AMOUNT_RANGE), 2),
                offer_apr=round(rng.uniform(rate_low, rate_high), 4),
                created_at=created_date.isoformat(),
                expires_at=(created_date + timedelta(days=OFFER_VALIDITY_DAYS)).isoformat(),
            )
        )
    return offers


def generate_applications(
    rng: random.Random, customers_by_id: dict[str, Customer], offers: list[PrequalOffer], as_of_date: date
) -> list[Application]:
    """Applications mostly follow an offer; a smaller "organic" share apply with no offer."""
    id_gen = _IdGenerator("APP", 7)
    applications: list[Application] = []
    offer_applied_customer_ids: set[str] = set()

    for offer in offers:
        if rng.random() >= APPLICATION_RATE_GIVEN_OFFER:
            continue
        created_date = date.fromisoformat(offer.created_at)
        submitted_date = created_date + timedelta(days=rng.randint(*APPLICATION_SUBMIT_DELAY_DAYS))
        status = weighted_choice(rng, APPLICATION_STATUS_WEIGHTS)
        applications.append(
            Application(
                application_id=id_gen.next_id(),
                customer_id=offer.customer_id,
                offer_id=offer.offer_id,
                requested_amount=offer.offer_amount,
                submitted_at=submitted_date.isoformat(),
                application_status=status,
            )
        )
        offer_applied_customer_ids.add(offer.customer_id)

    latest_application_date = as_of_date - timedelta(days=FUNNEL_TAIL_BUFFER_DAYS)
    for customer in customers_by_id.values():
        if customer.customer_id in offer_applied_customer_ids:
            continue
        if rng.random() >= ORGANIC_APPLICATION_RATE:
            continue
        customer_created = date.fromisoformat(customer.created_at)
        if customer_created > latest_application_date:
            continue
        submitted_date = _random_date_between(rng, customer_created, latest_application_date)
        status = weighted_choice(rng, APPLICATION_STATUS_WEIGHTS)
        applications.append(
            Application(
                application_id=id_gen.next_id(),
                customer_id=customer.customer_id,
                offer_id=None,
                requested_amount=round(rng.uniform(*PRINCIPAL_AMOUNT_RANGE), 2),
                submitted_at=submitted_date.isoformat(),
                application_status=status,
            )
        )
    return applications


def generate_underwriting_decisions(
    rng: random.Random,
    applications: list[Application],
    customers_by_id: dict[str, Customer],
    as_of_date: date,
) -> list[UnderwritingDecision]:
    """One decision per DECISIONED application. SUBMITTED/WITHDRAWN applications get none."""
    id_gen = _IdGenerator("DEC", 7)
    decisions: list[UnderwritingDecision] = []
    model_cutover = as_of_date - timedelta(days=MODEL_CUTOVER_DAYS_BEFORE_AS_OF)

    for application in applications:
        if application.application_status != ApplicationStatus.DECISIONED:
            continue
        customer = customers_by_id[application.customer_id]
        submitted_date = date.fromisoformat(application.submitted_at)
        decided_date = submitted_date + timedelta(days=rng.randint(*DECISION_DELAY_DAYS))
        decision = weighted_choice(rng, DECISION_WEIGHTS_BY_RISK[customer.risk_segment])
        model_version = "uw-model-v1" if decided_date < model_cutover else "uw-model-v2"

        rejection_reason = None
        approved_amount = None
        approved_apr = None
        if decision == Decision.REJECTED:
            rejection_reason = weighted_choice(rng, REJECTION_REASON_WEIGHTS)
        elif decision == Decision.APPROVED:
            approved_amount = round(application.requested_amount * rng.uniform(*APPROVED_AMOUNT_FACTOR_RANGE), 2)
            rate_low, rate_high = INTEREST_RATE_RANGE_BY_RISK[customer.risk_segment]
            approved_apr = round(rng.uniform(rate_low, rate_high), 4)

        decisions.append(
            UnderwritingDecision(
                decision_id=id_gen.next_id(),
                application_id=application.application_id,
                decision=decision,
                rejection_reason=rejection_reason,
                approved_amount=approved_amount,
                approved_apr=approved_apr,
                model_version=model_version,
                decided_at=decided_date.isoformat(),
            )
        )
    return decisions


def generate_loans(
    rng: random.Random,
    decisions: list[UnderwritingDecision],
    applications_by_id: dict[str, Application],
    customers_by_id: dict[str, Customer],
    as_of_date: date,
) -> list[Loan]:
    """One loan per APPROVED decision that actually funds. loan_status is derived from
    elapsed time since origination relative to term_months, not assigned independently.
    """
    loans = []
    counter = 1
    for decision in decisions:
        if decision.decision != Decision.APPROVED:
            continue
        if rng.random() >= FUNDED_RATE_GIVEN_APPROVED:
            continue

        application = applications_by_id[decision.application_id]
        customer = customers_by_id[application.customer_id]

        decided_date = date.fromisoformat(decision.decided_at)
        originated_date = decided_date + timedelta(days=rng.randint(*LOAN_FUNDING_DELAY_DAYS))
        if originated_date > as_of_date:
            originated_date = as_of_date

        term_months = weighted_choice(rng, TERM_MONTHS_WEIGHTS)
        elapsed_months = _months_between(originated_date, as_of_date)

        if elapsed_months >= term_months:
            loan_status = weighted_choice(rng, LOAN_STATUS_WEIGHTS_AT_TERM_END_BY_RISK[customer.risk_segment])
        elif elapsed_months >= 2 and rng.random() < LOAN_STATUS_EARLY_DEFAULT_PROBABILITY_BY_RISK[customer.risk_segment]:
            # Require >=2 elapsed months so at least 2 installments are already past due --
            # a loan can't show delinquency evidence (see
            # _generate_defaulted_loan_payment_events) before its first payment is even due.
            loan_status = LoanStatus.DEFAULTED
        else:
            loan_status = LoanStatus.ACTIVE

        principal_amount = decision.approved_amount or round(rng.uniform(*PRINCIPAL_AMOUNT_RANGE), 2)
        rate_low, rate_high = INTEREST_RATE_RANGE_BY_RISK[customer.risk_segment]
        interest_rate = decision.approved_apr or round(rng.uniform(rate_low, rate_high), 4)
        scheduled_payment_amount = round(principal_amount / term_months, 2)

        loans.append(
            Loan(
                loan_id=f"L{counter:06d}",
                application_id=application.application_id,
                customer_id=customer.customer_id,
                principal_amount=principal_amount,
                interest_rate=interest_rate,
                term_months=term_months,
                originated_at=originated_date.isoformat(),
                loan_status=loan_status,
                scheduled_payment_amount=scheduled_payment_amount,
            )
        )
        counter += 1
    return loans


def generate_payment_schedule(loans: list[Loan]) -> list[PaymentScheduleEntry]:
    """Every loan's full set of expected installments -- purely derived from
    loan fields, no randomness needed.
    """
    id_gen = _IdGenerator("SCH", 8)
    schedule_entries = []
    for loan in loans:
        origin = date.fromisoformat(loan.originated_at)
        for installment_number in range(1, loan.term_months + 1):
            due_date = add_months(origin, installment_number)
            schedule_entries.append(
                PaymentScheduleEntry(
                    schedule_id=id_gen.next_id(),
                    loan_id=loan.loan_id,
                    installment_number=installment_number,
                    due_date=due_date.isoformat(),
                    scheduled_amount=loan.scheduled_payment_amount,
                )
            )
    return schedule_entries


def _build_payment_event(
    rng: random.Random,
    event_id: str,
    schedule_entry: PaymentScheduleEntry,
    status: PaymentEventStatus,
    as_of_date: date,
) -> PaymentEvent:
    """Build one realized payment event consistent with the given status's rules.

    A LATE/FAILED payment_date is never allowed past as_of_date -- this is a
    point-in-time snapshot, so nothing can already be recorded as having
    happened in the future relative to it.
    """
    due_date = date.fromisoformat(schedule_entry.due_date)
    amount_due = schedule_entry.scheduled_amount

    if status == PaymentEventStatus.PAID:
        payment_date: date | None = due_date - timedelta(days=rng.randint(0, 5))
        amount = amount_due
    elif status == PaymentEventStatus.LATE:
        payment_date = min(due_date + timedelta(days=rng.randint(1, 30)), as_of_date)
        amount = amount_due
    elif status == PaymentEventStatus.FAILED:
        payment_date = min(due_date + timedelta(days=rng.randint(0, 5)), as_of_date)
        amount = 0.0
    else:  # MISSED
        payment_date = None
        amount = 0.0

    return PaymentEvent(
        event_id=event_id,
        schedule_id=schedule_entry.schedule_id,
        loan_id=schedule_entry.loan_id,
        event_type=PaymentEventType.PAYMENT,
        payment_date=payment_date.isoformat() if payment_date else None,
        amount=amount,
        payment_status=status,
        payment_method=weighted_choice(rng, PAYMENT_METHOD_WEIGHTS),
    )


def _generate_closed_loan_payment_events(
    rng: random.Random, loan: Loan, schedule_entries: list[PaymentScheduleEntry], as_of_date: date, id_gen: _IdGenerator
) -> list[PaymentEvent]:
    """CLOSED loans: every scheduled installment succeeds, and the final one is
    nudged so cumulative amount equals principal_amount within $0.01.
    """
    records = [
        _build_payment_event(rng, id_gen.next_id(), entry, PaymentEventStatus.PAID, as_of_date)
        for entry in schedule_entries
    ]
    total_paid = round(sum(r.amount for r in records), 2)
    shortfall = round(loan.principal_amount - total_paid, 2)
    if shortfall != 0 and records:
        last = records[-1]
        records[-1] = replace(last, amount=round(last.amount + shortfall, 2))
    return records


def _generate_defaulted_loan_payment_events(
    rng: random.Random,
    schedule_entries: list[PaymentScheduleEntry],
    as_of_date: date,
    risk_segment: RiskSegment,
    id_gen: _IdGenerator,
) -> list[PaymentEvent]:
    """DEFAULTED loans: history up to the point of default, then generation
    stops. The last generated event is forced LATE or MISSED so default is evidenced.

    Only installments due strictly BEFORE as_of_date are eligible for a
    LATE/MISSED outcome -- a bill due today can't yet be known as overdue.
    Never pads from future-dated schedule entries to reach a minimum count --
    generate_loans() guarantees (via its elapsed_months >= 2 guard on the
    early-default roll) that a DEFAULTED loan always has at least 2 past-due
    installments by the time this runs, so no fallback is needed here.
    """
    past_entries = [e for e in schedule_entries if date.fromisoformat(e.due_date) < as_of_date]
    windowed = past_entries[-DEFAULTED_HISTORY_WINDOW:]

    forced_index = len(windowed) - 1
    records = []
    for i, entry in enumerate(windowed):
        if i == forced_index:
            status = rng.choice([PaymentEventStatus.LATE, PaymentEventStatus.MISSED])
        else:
            status = weighted_choice(rng, PAYMENT_EVENT_OUTCOME_WEIGHTS_BY_RISK[risk_segment])
        records.append(_build_payment_event(rng, id_gen.next_id(), entry, status, as_of_date))
    return records


def _generate_active_loan_payment_events(
    rng: random.Random,
    schedule_entries: list[PaymentScheduleEntry],
    as_of_date: date,
    risk_segment: RiskSegment,
    id_gen: _IdGenerator,
) -> list[PaymentEvent]:
    """ACTIVE loans: only a recent window of past-due installments get a realized
    event. Future-dated (and today-dated) installments have no event yet --
    payment_schedule already carries the expectation; payment_events only
    records what actually happened, and "happened" requires the due date to
    have already passed.
    """
    past_entries = [e for e in schedule_entries if date.fromisoformat(e.due_date) < as_of_date][-ACTIVE_PAST_WINDOW:]

    records = []
    for entry in past_entries:
        status = weighted_choice(rng, PAYMENT_EVENT_OUTCOME_WEIGHTS_BY_RISK[risk_segment])
        records.append(_build_payment_event(rng, id_gen.next_id(), entry, status, as_of_date))
    return records


def _apply_reversals(
    rng: random.Random, payment_events: list[PaymentEvent], as_of_date: date, id_gen: _IdGenerator
) -> list[PaymentEvent]:
    """A small fraction of PAID events later get an offsetting REVERSAL event --
    same schedule_id, negative amount, payment_status=REVERSED. Never dated past
    as_of_date, for the same reason payment_date is capped elsewhere.
    """
    reversal_events = []
    for event in payment_events:
        if event.payment_status != PaymentEventStatus.PAID:
            continue
        if rng.random() >= REVERSAL_RATE_GIVEN_PAID:
            continue
        payment_date = date.fromisoformat(event.payment_date)
        reversal_date = min(payment_date + timedelta(days=rng.randint(*REVERSAL_DELAY_DAYS)), as_of_date)
        reversal_events.append(
            PaymentEvent(
                event_id=id_gen.next_id(),
                schedule_id=event.schedule_id,
                loan_id=event.loan_id,
                event_type=PaymentEventType.REVERSAL,
                payment_date=reversal_date.isoformat(),
                amount=-event.amount,
                payment_status=PaymentEventStatus.REVERSED,
                payment_method=event.payment_method,
            )
        )
    return reversal_events


def generate_payment_events(
    rng: random.Random,
    loans: list[Loan],
    schedule_by_loan: dict[str, list[PaymentScheduleEntry]],
    customers_by_id: dict[str, Customer],
    as_of_date: date,
) -> list[PaymentEvent]:
    """Generate realized payment events for every loan, branching on loan_status,
    then layer in a small number of reversals.
    """
    id_gen = _IdGenerator("PEV", 8)
    payment_events: list[PaymentEvent] = []

    for loan in loans:
        customer = customers_by_id[loan.customer_id]
        schedule_entries = schedule_by_loan.get(loan.loan_id, [])

        if loan.loan_status == LoanStatus.CLOSED:
            payment_events.extend(_generate_closed_loan_payment_events(rng, loan, schedule_entries, as_of_date, id_gen))
        elif loan.loan_status == LoanStatus.DEFAULTED:
            payment_events.extend(
                _generate_defaulted_loan_payment_events(
                    rng, schedule_entries, as_of_date, customer.risk_segment, id_gen
                )
            )
        else:
            payment_events.extend(
                _generate_active_loan_payment_events(rng, schedule_entries, as_of_date, customer.risk_segment, id_gen)
            )

    payment_events.extend(_apply_reversals(rng, payment_events, as_of_date, id_gen))
    return payment_events


def generate_delinquency_events(
    rng: random.Random,
    loans: list[Loan],
    payment_events_by_loan: dict[str, list[PaymentEvent]],
    as_of_date: date,
) -> list[DelinquencyEvent]:
    """A delinquency snapshot for loans with at least one LATE/MISSED payment event."""
    id_gen = _IdGenerator("DLQ", 7)
    delinquency_events = []
    for loan in loans:
        events = payment_events_by_loan.get(loan.loan_id, [])
        troubled = [e for e in events if e.payment_status in (PaymentEventStatus.LATE, PaymentEventStatus.MISSED)]
        if not troubled:
            continue
        bucket = (
            DelinquencyBucket.DAYS_90_PLUS
            if loan.loan_status == LoanStatus.DEFAULTED
            else DELINQUENCY_BUCKET_BY_STATUS[troubled[-1].payment_status]
        )
        low, high = DELINQUENCY_DAYS_PAST_DUE_RANGE_BY_BUCKET[bucket]
        delinquency_events.append(
            DelinquencyEvent(
                delinquency_id=id_gen.next_id(),
                loan_id=loan.loan_id,
                as_of_date=as_of_date.isoformat(),
                days_past_due=rng.randint(low, high),
                bucket=bucket,
            )
        )
    return delinquency_events


def generate_defaults(
    rng: random.Random,
    loans: list[Loan],
    payment_events_by_loan: dict[str, list[PaymentEvent]],
    as_of_date: date,
) -> list[Default]:
    """One row per DEFAULTED loan, with a small chance of a partial recovery."""
    id_gen = _IdGenerator("DEF", 6)
    defaults = []
    for loan in loans:
        if loan.loan_status != LoanStatus.DEFAULTED:
            continue
        events = payment_events_by_loan.get(loan.loan_id, [])
        paid_total = sum(e.amount for e in events if e.payment_status == PaymentEventStatus.PAID)
        realized_dates = [date.fromisoformat(e.payment_date) for e in events if e.payment_date]
        default_date = (
            max(realized_dates) + timedelta(days=rng.randint(5, 20))
            if realized_dates
            else date.fromisoformat(loan.originated_at)
        )
        if default_date > as_of_date:
            default_date = as_of_date
        balance_at_default = round(max(loan.principal_amount - paid_total, 0.0), 2)

        recovery_amount = 0.0
        recovery_date = None
        if balance_at_default > 0 and rng.random() < RECOVERY_PROBABILITY:
            recovery_amount = round(balance_at_default * rng.uniform(*RECOVERY_FACTOR_RANGE), 2)
            recovery_date_candidate = default_date + timedelta(days=rng.randint(*RECOVERY_DELAY_DAYS))
            recovery_date = min(recovery_date_candidate, as_of_date).isoformat()

        defaults.append(
            Default(
                default_id=id_gen.next_id(),
                loan_id=loan.loan_id,
                default_date=default_date.isoformat(),
                balance_at_default=balance_at_default,
                recovery_amount=recovery_amount,
                recovery_date=recovery_date,
            )
        )
    return defaults


def write_json(path: Path, records: list[dict]) -> None:
    """Write records as a JSON array with 2-space indentation and a trailing newline."""
    with path.open("w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
        f.write("\n")


def print_summary(dataset: dict[str, list]) -> None:
    """Print a human-readable generation summary across all 12 tables."""
    print("Generation summary")
    for table_name, records in dataset.items():
        print(f"  {table_name:<24} {len(records)}")

    loan_status_counts = Counter(loan.loan_status.value for loan in dataset["loans"])
    print("  loan status counts:")
    for status in LoanStatus:
        print(f"    {status.value:<12} {loan_status_counts.get(status.value, 0)}")

    payment_status_counts = Counter(e.payment_status.value for e in dataset["payment_events"])
    print("  payment_event status counts:")
    for status in PaymentEventStatus:
        print(f"    {status.value:<12} {payment_status_counts.get(status.value, 0)}")

    email_type_counts = Counter(e.event_type.value for e in dataset["email_events"])
    print("  email_event type counts:")
    for event_type in EmailEventType:
        print(f"    {event_type.value:<12} {email_type_counts.get(event_type.value, 0)}")

    decision_counts = Counter(d.decision.value for d in dataset["underwriting_decisions"])
    print("  underwriting decision counts:")
    for decision in Decision:
        print(f"    {decision.value:<14} {decision_counts.get(decision.value, 0)}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and validate CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Generate synthetic full-lifecycle banking data (12 tables)."
    )
    parser.add_argument("--num-customers", type=int, default=DEFAULT_NUM_CUSTOMERS)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--as-of-date", type=str, default=DEFAULT_AS_OF_DATE)
    args = parser.parse_args(argv)

    if args.num_customers <= 0:
        parser.error("--num-customers must be a positive integer")

    try:
        as_of_date = date.fromisoformat(args.as_of_date)
    except ValueError as exc:
        parser.error(f"--as-of-date must be an ISO date (YYYY-MM-DD): {exc}")
        raise
    args.as_of_date = as_of_date

    return args


def generate_dataset(num_customers: int, seed: int, as_of_date: date) -> dict[str, list]:
    """Generate the full 12-table lifecycle dataset for given inputs.

    Returns a dict keyed by table name (customers, campaigns, coupon_rules,
    email_events, prequal_offers, applications, underwriting_decisions, loans,
    payment_schedule, payment_events, delinquency_events, defaults), values
    lists of the corresponding dataclass instances.
    """
    rng = random.Random(seed)
    earliest_created_at = as_of_date - timedelta(days=CUSTOMER_HISTORY_DAYS)

    customers = generate_customers(rng, num_customers, as_of_date)
    customers_by_id = {c.customer_id: c for c in customers}

    campaigns = generate_campaigns(rng, as_of_date, num_customers, earliest_created_at)
    coupon_rules = generate_coupon_rules(rng, campaigns)
    coupon_rules_by_campaign: dict[str, list[CouponRule]] = {}
    for coupon_rule in coupon_rules:
        coupon_rules_by_campaign.setdefault(coupon_rule.campaign_id, []).append(coupon_rule)

    email_events, clicked_at = generate_email_events(rng, customers, campaigns, as_of_date)

    offers = generate_prequal_offers(rng, customers_by_id, clicked_at, coupon_rules_by_campaign, as_of_date)

    applications = generate_applications(rng, customers_by_id, offers, as_of_date)
    applications_by_id = {a.application_id: a for a in applications}

    decisions = generate_underwriting_decisions(rng, applications, customers_by_id, as_of_date)

    loans = generate_loans(rng, decisions, applications_by_id, customers_by_id, as_of_date)

    payment_schedule = generate_payment_schedule(loans)
    schedule_by_loan: dict[str, list[PaymentScheduleEntry]] = {}
    for entry in payment_schedule:
        schedule_by_loan.setdefault(entry.loan_id, []).append(entry)

    payment_events = generate_payment_events(rng, loans, schedule_by_loan, customers_by_id, as_of_date)
    payment_events_by_loan: dict[str, list[PaymentEvent]] = {}
    for event in payment_events:
        payment_events_by_loan.setdefault(event.loan_id, []).append(event)

    delinquency_events = generate_delinquency_events(rng, loans, payment_events_by_loan, as_of_date)
    defaults = generate_defaults(rng, loans, payment_events_by_loan, as_of_date)

    return {
        "customers": customers,
        "campaigns": campaigns,
        "coupon_rules": coupon_rules,
        "email_events": email_events,
        "prequal_offers": offers,
        "applications": applications,
        "underwriting_decisions": decisions,
        "loans": loans,
        "payment_schedule": payment_schedule,
        "payment_events": payment_events,
        "delinquency_events": delinquency_events,
        "defaults": defaults,
    }


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    dataset = generate_dataset(args.num_customers, args.seed, args.as_of_date)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for table_name, records in dataset.items():
        id_field = TABLE_ID_FIELDS[table_name]
        write_json(
            output_dir / f"{table_name}.json",
            sorted((r.to_dict() for r in records), key=lambda r, f=id_field: r[f]),
        )

    print_summary(dataset)


if __name__ == "__main__":
    main()
