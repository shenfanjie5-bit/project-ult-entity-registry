"""Runtime models used during mention resolution."""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, Field

from entity_registry.core import FinalStatus, ResolutionMethod


def _utcnow() -> datetime:
    return datetime.now(UTC)


class MentionCandidateSet(BaseModel):
    """Candidates collected while resolving one raw mention."""

    raw_mention_text: str
    deterministic_hits: list[str]
    fuzzy_hits: list[str]
    llm_required: bool
    final_status: FinalStatus


class ResolutionContext(BaseModel):
    """Context supplied to a single mention resolution attempt."""

    raw_mention_text: str
    document_context: str
    source_type: str
    timestamp: datetime = Field(default_factory=_utcnow)


class ResolutionDecision(BaseModel):
    """Decision output from a single resolution attempt."""

    selected_entity_id: str | None
    method: ResolutionMethod
    confidence: float | None
    rationale: str


class BatchResolutionJob(BaseModel):
    """Runtime metadata for a batch resolution job."""

    job_id: str
    reference_ids: list[str]
    status: str
    created_at: datetime = Field(default_factory=_utcnow)
    completed_at: datetime | None = None
