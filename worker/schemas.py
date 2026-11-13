"""worker/schemas.py

Row-level validation and normalisation. Normalising BEFORE validation matters:
the database uniqueness rule is on (email, bib_number), and "Foo@X.com" vs
"foo@x.com" would otherwise slip past it as two different people.
"""

from enum import Enum

from pydantic import BaseModel, EmailStr, Field, field_validator


class RaceCategory(str, Enum):
    """Supported race categories (matches the race_category_enum database type)."""

    FULL_MARATHON = "FULL_MARATHON"
    HALF_MARATHON = "HALF_MARATHON"
    TEN_K = "10K"
    FIVE_K = "5K"


class ParticipantRowSchema(BaseModel):
    first_name: str = Field(..., min_length=1, max_length=100)
    last_name: str = Field(..., min_length=1, max_length=100)
    email: EmailStr
    bib_number: str = Field(..., min_length=1, max_length=50)
    category: RaceCategory
    registration_fee: float = Field(..., ge=0.0, allow_inf_nan=False)

    @field_validator("first_name", "last_name", mode="before")
    @classmethod
    def _strip_names(cls, v):
        return v.strip() if isinstance(v, str) else v

    @field_validator("email", mode="before")
    @classmethod
    def _normalise_email(cls, v):
        return v.strip().lower() if isinstance(v, str) else v

    @field_validator("bib_number", mode="before")
    @classmethod
    def _normalise_bib(cls, v):
        # Excel often stores numeric bibs as 1001 or 1001.0
        if isinstance(v, float) and v.is_integer():
            v = int(v)
        if isinstance(v, int) and not isinstance(v, bool):
            v = str(v)
        return v.strip().upper() if isinstance(v, str) else v

    @field_validator("category", mode="before")
    @classmethod
    def _normalise_category(cls, v):
        return v.strip().upper() if isinstance(v, str) else v
