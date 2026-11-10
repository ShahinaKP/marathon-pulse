"""worker/schemas.py

Validation models and enumeration schemas for the background worker service.
Defines data structures and field constraints for marathon participant row
ingestion, guaranteeing strict parity with PostgreSQL database schema and ENUMs.
"""

from enum import Enum
from pydantic import BaseModel, EmailStr, Field


class RaceCategory(str, Enum):
    """Supported marathon event categories matching PostgreSQL database ENUMs."""

    FULL_MARATHON = "FULL_MARATHON"
    HALF_MARATHON = "HALF_MARATHON"
    TEN_K = "10K"
    FIVE_K = "5K"


class ParticipantRowSchema(BaseModel):
    """Validation schema for an individual participant row extracted from an Excel sheet.

    Attributes:
        first_name: Given name of the marathon registrant.
        last_name: Surname or family name of the registrant.
        email: Standard RFC-compliant email address.
        bib_number: Assigned race number identifier.
        category: Registered marathon category constrained by RaceCategory ENUM.
        registration_fee: Currency amount paid for the race entry.
    """

    first_name: str = Field(..., min_length=1, max_length=100)
    last_name: str = Field(..., min_length=1, max_length=100)
    email: EmailStr
    bib_number: str = Field(..., min_length=1, max_length=50)
    category: RaceCategory
    registration_fee: float = Field(..., ge=0.0)