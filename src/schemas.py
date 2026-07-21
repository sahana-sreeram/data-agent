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
