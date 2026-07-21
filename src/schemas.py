"""Typed schema definitions for the data-agent synthetic dataset.

Defines the enums and record types for customers, loans, and payments so
generation logic (and later consumers such as ETL/agent code) share one
source of truth for valid field values.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum


class IncomeBand(str, Enum):
    UNDER_40000 = "UNDER_40000"
    RANGE_40000_60000 = "40000_60000"
    RANGE_60000_80000 = "60000_80000"
    RANGE_80000_120000 = "80000_120000"
    OVER_120000 = "OVER_120000"


class CreditScoreBand(str, Enum):
    UNDER_620 = "UNDER_620"
    RANGE_620_679 = "620_679"
    RANGE_680_719 = "680_719"
    RANGE_720_759 = "720_759"
    PLUS_760 = "760_PLUS"


class RiskSegment(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class LoanStatus(str, Enum):
    ACTIVE = "ACTIVE"
    CLOSED = "CLOSED"
    DEFAULTED = "DEFAULTED"


class PaymentStatus(str, Enum):
    SCHEDULED = "SCHEDULED"
    PAID = "PAID"
    LATE = "LATE"
    MISSED = "MISSED"
    FAILED = "FAILED"


class PaymentMethod(str, Enum):
    ACH = "ACH"
    CARD = "CARD"
    CHECK = "CHECK"


class Channel(str, Enum):
    EMAIL = "EMAIL"
    SOCIAL = "SOCIAL"
    PARTNER = "PARTNER"


class DiscountType(str, Enum):
    RATE_DISCOUNT = "RATE_DISCOUNT"
    FEE_WAIVER = "FEE_WAIVER"


class EmailEventType(str, Enum):
    SENT = "SENT"
    OPENED = "OPENED"
    CLICKED = "CLICKED"


class ApplicationStatus(str, Enum):
    SUBMITTED = "SUBMITTED"
    WITHDRAWN = "WITHDRAWN"
    DECISIONED = "DECISIONED"


class Decision(str, Enum):
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    MANUAL_REVIEW = "MANUAL_REVIEW"


class RejectionReason(str, Enum):
    LOW_CREDIT_SCORE = "LOW_CREDIT_SCORE"
    HIGH_DEBT_TO_INCOME = "HIGH_DEBT_TO_INCOME"
    INSUFFICIENT_INCOME = "INSUFFICIENT_INCOME"
    INCOMPLETE_APPLICATION = "INCOMPLETE_APPLICATION"
    FRAUD_RISK = "FRAUD_RISK"


class PaymentEventType(str, Enum):
    PAYMENT = "PAYMENT"
    REVERSAL = "REVERSAL"


class PaymentEventStatus(str, Enum):
    PAID = "PAID"
    LATE = "LATE"
    MISSED = "MISSED"
    FAILED = "FAILED"
    REVERSED = "REVERSED"


class DelinquencyBucket(str, Enum):
    CURRENT = "CURRENT"
    DAYS_30 = "30"
    DAYS_60 = "60"
    DAYS_90_PLUS = "90_PLUS"


TERM_MONTHS_CHOICES: tuple[int, ...] = (12, 24, 36, 48, 60)

US_STATE_CODES: tuple[str, ...] = (
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
)


@dataclass(frozen=True)
class Customer:
    customer_id: str
    created_at: str
    state: str
    income_band: IncomeBand
    credit_score_band: CreditScoreBand
    credit_score: int
    risk_segment: RiskSegment

    def to_dict(self) -> dict:
        d = asdict(self)
        d["income_band"] = self.income_band.value
        d["credit_score_band"] = self.credit_score_band.value
        d["risk_segment"] = self.risk_segment.value
        return d


@dataclass(frozen=True)
class Loan:
    loan_id: str
    application_id: str
    customer_id: str
    principal_amount: float
    interest_rate: float
    term_months: int
    originated_at: str
    loan_status: LoanStatus
    scheduled_payment_amount: float

    def to_dict(self) -> dict:
        d = asdict(self)
        d["loan_status"] = self.loan_status.value
        return d


@dataclass(frozen=True)
class Payment:
    payment_id: str
    loan_id: str
    due_date: str
    payment_date: str | None
    amount_due: float
    amount_paid: float
    payment_status: PaymentStatus
    payment_method: PaymentMethod

    def to_dict(self) -> dict:
        d = asdict(self)
        d["payment_status"] = self.payment_status.value
        d["payment_method"] = self.payment_method.value
        return d


@dataclass(frozen=True)
class Campaign:
    campaign_id: str
    name: str
    channel: Channel
    start_date: str
    end_date: str
    target_risk_segment: RiskSegment | None

    def to_dict(self) -> dict:
        d = asdict(self)
        d["channel"] = self.channel.value
        d["target_risk_segment"] = self.target_risk_segment.value if self.target_risk_segment else None
        return d


@dataclass(frozen=True)
class CouponRule:
    coupon_rule_id: str
    coupon_code: str
    campaign_id: str
    discount_type: DiscountType
    discount_value: float
    valid_from: str
    valid_to: str

    def to_dict(self) -> dict:
        d = asdict(self)
        d["discount_type"] = self.discount_type.value
        return d


@dataclass(frozen=True)
class EmailEvent:
    event_id: str
    campaign_id: str
    customer_id: str
    event_type: EmailEventType
    event_timestamp: str

    def to_dict(self) -> dict:
        d = asdict(self)
        d["event_type"] = self.event_type.value
        return d


@dataclass(frozen=True)
class PrequalOffer:
    offer_id: str
    customer_id: str
    campaign_id: str | None
    coupon_code: str | None
    offer_amount: float
    offer_apr: float
    created_at: str
    expires_at: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class Application:
    application_id: str
    customer_id: str
    offer_id: str | None
    requested_amount: float
    submitted_at: str
    application_status: ApplicationStatus

    def to_dict(self) -> dict:
        d = asdict(self)
        d["application_status"] = self.application_status.value
        return d


@dataclass(frozen=True)
class UnderwritingDecision:
    decision_id: str
    application_id: str
    decision: Decision
    rejection_reason: RejectionReason | None
    approved_amount: float | None
    approved_apr: float | None
    model_version: str
    decided_at: str

    def to_dict(self) -> dict:
        d = asdict(self)
        d["decision"] = self.decision.value
        d["rejection_reason"] = self.rejection_reason.value if self.rejection_reason else None
        return d


@dataclass(frozen=True)
class PaymentScheduleEntry:
    schedule_id: str
    loan_id: str
    installment_number: int
    due_date: str
    scheduled_amount: float

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class PaymentEvent:
    event_id: str
    schedule_id: str | None
    loan_id: str
    event_type: PaymentEventType
    payment_date: str | None
    amount: float
    payment_status: PaymentEventStatus
    payment_method: PaymentMethod

    def to_dict(self) -> dict:
        d = asdict(self)
        d["event_type"] = self.event_type.value
        d["payment_status"] = self.payment_status.value
        d["payment_method"] = self.payment_method.value
        return d


@dataclass(frozen=True)
class DelinquencyEvent:
    delinquency_id: str
    loan_id: str
    as_of_date: str
    days_past_due: int
    bucket: DelinquencyBucket

    def to_dict(self) -> dict:
        d = asdict(self)
        d["bucket"] = self.bucket.value
        return d


@dataclass(frozen=True)
class Default:
    default_id: str
    loan_id: str
    default_date: str
    balance_at_default: float
    recovery_amount: float
    recovery_date: str | None

    def to_dict(self) -> dict:
        return asdict(self)
