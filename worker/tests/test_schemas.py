"""Row validation and normalisation. Needs the real pydantic (installed via requirements.txt)."""
import pytest
from pydantic import ValidationError

from schemas import ParticipantRowSchema, RaceCategory


def row(**overrides):
    base = dict(
        first_name="Ann", last_name="Lee", email="ann@marathon.com",
        bib_number="1001", category="10K", registration_fee=45.0,
    )
    base.update(overrides)
    return base


def test_valid_row_passes():
    r = ParticipantRowSchema(**row())
    assert r.category is RaceCategory.TEN_K
    assert r.registration_fee == 45.0


def test_email_is_trimmed_and_lowercased():
    assert ParticipantRowSchema(**row(email="  Ann@Marathon.COM ")).email == "ann@marathon.com"


@pytest.mark.parametrize("raw, expected", [(1001.0, "1001"), (1001, "1001"), (" bib-7 ", "BIB-7")])
def test_bib_number_is_normalised(raw, expected):
    assert ParticipantRowSchema(**row(bib_number=raw)).bib_number == expected


def test_category_is_case_insensitive_and_trimmed():
    assert ParticipantRowSchema(**row(category=" full_marathon ")).category is RaceCategory.FULL_MARATHON


def test_names_are_trimmed():
    r = ParticipantRowSchema(**row(first_name="  Ann ", last_name=" Lee  "))
    assert (r.first_name, r.last_name) == ("Ann", "Lee")


def test_normalisation_makes_duplicates_comparable():
    a = ParticipantRowSchema(**row(email="Ann@Marathon.com", bib_number=1001.0))
    b = ParticipantRowSchema(**row(email=" ann@marathon.com ", bib_number="1001"))
    assert (a.email, a.bib_number) == (b.email, b.bib_number)


@pytest.mark.parametrize(
    "overrides",
    [
        dict(email="not-an-email"),
        dict(email=None),
        dict(registration_fee="NOT_A_NUMBER"),
        dict(registration_fee=-1),
        dict(registration_fee=1e9),            # would overflow NUMERIC(10,2)
        dict(registration_fee=float("nan")),
        dict(bib_number="   "),                # blank after trimming
        dict(bib_number=None),
        dict(category="ULTRA_MARATHON"),
        dict(first_name=""),
        dict(first_name="x" * 101),
    ],
)
def test_invalid_rows_are_rejected(overrides):
    with pytest.raises(ValidationError):
        ParticipantRowSchema(**row(**overrides))


def test_error_message_names_the_field():
    import processor

    with pytest.raises(ValidationError) as exc:
        ParticipantRowSchema(**row(email="nope", registration_fee=-5))
    text = processor.format_validation_error(exc.value)
    assert "email" in text and "registration_fee" in text
