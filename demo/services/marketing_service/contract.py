"""marketing_service's data contract: the customer-acquisition side of the lifecycle
(customer profiles, campaigns, coupon rules, email engagement, prequalification offers).

Event types: CustomerProfileObserved, CampaignCreated, CouponRuleDefined, EmailSent,
EmailOpened, EmailClicked, PrequalificationCreated.
"""

from __future__ import annotations

SCHEMA_VERSION = "v1"

EMAIL_EVENT_TYPE_MAP = {
    "SENT": "EmailSent",
    "OPENED": "EmailOpened",
    "CLICKED": "EmailClicked",
}
