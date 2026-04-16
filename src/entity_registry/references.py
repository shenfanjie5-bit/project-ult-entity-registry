"""Reference and resolution-case audit models."""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, Field

from entity_registry.core import DecisionType, ResolutionMethod


def _utcnow() -> datetime:
    return datetime.now(UTC)


class EntityReference(BaseModel):
    """A recorded raw mention and its resolution state."""

    reference_id: str
    raw_mention_text: str
    source_context: dict
    resolved_entity_id: str | None
    resolution_method: ResolutionMethod
    resolution_confidence: float | None
    created_at: datetime = Field(default_factory=_utcnow)


class ResolutionCase(BaseModel):
    """Auditable decision record for a mention resolution case."""

    case_id: str
    reference_id: str
    candidate_entity_ids: list[str]
    selected_entity_id: str | None
    decision_type: DecisionType
    decision_rationale: str
    created_at: datetime = Field(default_factory=_utcnow)
