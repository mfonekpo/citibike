import re
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
)


REFUSAL = "I don't have enough warehouse data to answer that."
SOURCE_TABLE = "citibike.fct_trips"
METRIC = "monthly_trip_count"


class GuardrailError(ValueError):
    """The request or evidence is unsuitable for a grounded answer."""


def _validate_month(value: str) -> str:
    if (
        not re.fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", value)
        or value.startswith("0000-")
    ):
        raise ValueError("Month must be a valid YYYY-MM value.")

    return value


class QuestionIntent(BaseModel):
    """The schema the LLM will use to interpret a question."""

    model_config = ConfigDict(
        extra="forbid",
        strict=True,
    )

    scope: Literal[
        "monthly_trip_count",
        "unsupported",
    ]

    operation: Literal[
        "list",
        "max",
        "min",
        "total",
        "compare",
        "unsupported",
    ]

    months: list[str] = Field(
        max_length=120,
        description=(
            "Every month requested, in YYYY-MM format. Expand explicit "
            "years, quarters and ranges into all their months. Use [] "
            "only for a question about the available indexed months."
        ),
    )

    @field_validator("months")
    @classmethod
    def validate_months(cls, values: list[str]) -> list[str]:
        for value in values:
            _validate_month(value)

        if len(values) != len(set(values)):
            raise ValueError("Requested months must not repeat.")

        return values


class MonthlyTripCount(BaseModel):
    """A validated row from the existing Qdrant payload."""

    model_config = ConfigDict(
        extra="forbid",
        strict=True,
    )

    month: str
    trip_count: int = Field(ge=0)

    @field_validator("month")
    @classmethod
    def validate_month(cls, value: str) -> str:
        return _validate_month(value)


def validate_question(question: str) -> None:
    """A preliminary check, before LLM interpretation."""

    if (
        not isinstance(question, str)
        or not 3 <= len(question.strip()) <= 1000
    ):
        raise GuardrailError(
            "Question must contain 3 to 1000 characters."
        )

    # These topics are outside the current monthly-trip-count corpus.
    # This preliminary check works alongside intent/evidence validation.
    blocked_topics = (
        r"\b(?:recipes?|pancakes?|chocolate|weather|"
        r"temperatures?|rainfall|forecasts?)\b"
    )

    if re.search(
        blocked_topics,
        question,
        flags=re.IGNORECASE,
    ):
        raise GuardrailError(
            "The requested topic is outside this corpus."
        )


def _require_supported_intent(intent: QuestionIntent) -> None:
    if (
        intent.scope != METRIC
        or intent.operation == "unsupported"
    ):
        raise GuardrailError(
            "The requested operation is unsupported."
        )

    if (
        intent.operation == "compare"
        and len(intent.months) != 2
    ):
        raise GuardrailError(
            "Comparison requires two explicit months."
        )


def parse_intent(raw_json: str) -> QuestionIntent:
    """Validate the structured interpretation returned by the LLM."""

    if not isinstance(raw_json, str) or not raw_json.strip():
        raise GuardrailError(
            "The model returned no interpretation."
        )

    try:
        intent = QuestionIntent.model_validate_json(raw_json)
    except ValidationError as exc:
        raise GuardrailError(
            "The model interpretation is invalid."
        ) from exc

    _require_supported_intent(intent)

    return intent


def validate_evidence(
    hits: list[dict[str, Any]],
) -> dict[str, int]:
    """Read facts from structured rows rather than generated text."""

    counts: dict[str, int] = {}

    for hit in hits:
        if (
            hit.get("source_table") != SOURCE_TABLE
            or hit.get("metric") != METRIC
        ):
            continue

        rows = hit.get("rows")

        if not isinstance(rows, list) or not rows:
            raise GuardrailError(
                "Monthly evidence is empty or malformed."
            )

        for raw_row in rows:
            try:
                row = MonthlyTripCount.model_validate(raw_row)
            except ValidationError as exc:
                raise GuardrailError(
                    "A monthly evidence row is invalid."
                ) from exc

            if (
                row.month in counts
                and counts[row.month] != row.trip_count
            ):
                raise GuardrailError(
                    f"Conflicting trip counts exist for {row.month}."
                )

            # Equal duplicate rows count once.
            counts[row.month] = row.trip_count

    if not counts:
        raise GuardrailError(
            "No valid monthly-trip-count evidence was found."
        )

    return counts


def prepare_answer(
    intent: QuestionIntent,
    hits: list[dict[str, Any]],
) -> str:
    """Calculate a complete approved answer from validated evidence."""

    _require_supported_intent(intent)

    counts = validate_evidence(hits)
    selected = sorted(intent.months or counts.keys())

    if any(month not in counts for month in selected):
        raise GuardrailError(
            "Some requested months are not indexed."
        )

    scope = (
        f"Based on indexed data for {', '.join(selected)}. "
    )

    if intent.operation == "list":
        body = "; ".join(
            f"{month}: {counts[month]:,} trips"
            for month in selected
        ) + "."

    elif intent.operation == "total":
        total = sum(
            counts[month] for month in selected
        )

        body = (
            f"Total trips across these indexed months: {total:,}."
        )

    elif intent.operation in {"max", "min"}:
        choose = max if intent.operation == "max" else min

        value = choose(
            counts[month] for month in selected
        )

        winners = [
            month
            for month in selected
            if counts[month] == value
        ]

        label = (
            "Highest"
            if intent.operation == "max"
            else "Lowest"
        )

        body = (
            f"{label} monthly trip count: {value:,}. "
            f"Month(s): {', '.join(winners)}."
        )

    elif intent.operation == "compare":
        first, second = selected
        difference = counts[second] - counts[first]

        body = (
            f"{first}: {counts[first]:,} trips; "
            f"{second}: {counts[second]:,} trips. "
        )

        if difference == 0:
            body += (
                "Both months had the same number of trips."
            )
        else:
            direction = (
                "more" if difference > 0 else "fewer"
            )

            body += (
                f"{second} had {abs(difference):,} "
                f"{direction} trips than {first}."
            )

    else:
        raise GuardrailError(
            "The requested operation is unsupported."
        )

    return scope + body


def finalize_answer(
    candidate: str | None,
    approved_answer: str,
) -> str:
    """Accept only the exact approved answer.

    approved_answer must come from prepare_answer(),
    never from the LLM.
    """

    if (
        isinstance(candidate, str)
        and candidate.strip() == approved_answer
    ):
        return candidate.strip()

    return approved_answer