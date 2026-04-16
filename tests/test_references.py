from datetime import UTC, datetime

from entity_registry.core import DecisionType, FinalStatus, ResolutionMethod
from entity_registry.references import EntityReference, ResolutionCase
from entity_registry.resolution_types import (
    BatchResolutionJob,
    MentionCandidateSet,
    ResolutionContext,
    ResolutionDecision,
)


def test_entity_reference_builds_resolved_state() -> None:
    reference = EntityReference(
        reference_id="ref-1",
        raw_mention_text="CATL",
        source_context={"source": "fixture"},
        resolved_entity_id="ENT_STOCK_300750.SZ",
        resolution_method=ResolutionMethod.DETERMINISTIC,
        resolution_confidence=1.0,
        created_at=datetime(2026, 4, 15, tzinfo=UTC),
    )

    assert reference.resolved_entity_id == "ENT_STOCK_300750.SZ"
    assert reference.resolution_method is ResolutionMethod.DETERMINISTIC


def test_entity_reference_builds_unresolved_state() -> None:
    reference = EntityReference(
        reference_id="ref-2",
        raw_mention_text="Unknown Corp",
        source_context={"source": "fixture"},
        resolved_entity_id=None,
        resolution_method=ResolutionMethod.UNRESOLVED,
        resolution_confidence=None,
    )

    assert reference.resolved_entity_id is None
    assert reference.resolution_method is ResolutionMethod.UNRESOLVED


def test_entity_reference_round_trips_without_data_loss() -> None:
    reference = EntityReference(
        reference_id="ref-3",
        raw_mention_text="CATL",
        source_context={"document_id": "doc-1", "offset": 3},
        resolved_entity_id="ENT_STOCK_300750.SZ",
        resolution_method=ResolutionMethod.DETERMINISTIC,
        resolution_confidence=0.98,
        created_at=datetime(2026, 4, 15, tzinfo=UTC),
    )

    restored = EntityReference.model_validate(reference.model_dump(mode="json"))

    assert restored == reference


def test_resolution_case_builds() -> None:
    case = ResolutionCase(
        case_id="case-1",
        reference_id="ref-1",
        candidate_entity_ids=["ENT_STOCK_300750.SZ"],
        selected_entity_id="ENT_STOCK_300750.SZ",
        decision_type=DecisionType.AUTO,
        decision_rationale="single deterministic hit",
        created_at=datetime(2026, 4, 15, tzinfo=UTC),
    )

    assert case.selected_entity_id == "ENT_STOCK_300750.SZ"
    assert case.decision_type is DecisionType.AUTO


def test_resolution_case_supports_unresolved_selection() -> None:
    case = ResolutionCase(
        case_id="case-2",
        reference_id="ref-2",
        candidate_entity_ids=[],
        selected_entity_id=None,
        decision_type=DecisionType.MANUAL_REVIEW,
        decision_rationale="no candidates",
    )

    assert case.selected_entity_id is None
    assert case.candidate_entity_ids == []


def test_mention_candidate_set_builds_and_serializes() -> None:
    candidate_set = MentionCandidateSet(
        raw_mention_text="CATL",
        deterministic_hits=["ENT_STOCK_300750.SZ"],
        fuzzy_hits=[],
        llm_required=False,
        final_status=FinalStatus.RESOLVED,
    )

    payload = candidate_set.model_dump(mode="json")

    assert payload["final_status"] == "resolved"
    assert payload["deterministic_hits"] == ["ENT_STOCK_300750.SZ"]


def test_resolution_context_builds() -> None:
    context = ResolutionContext(
        raw_mention_text="CATL",
        document_context="CATL published its annual report",
        source_type="announcement",
        timestamp=datetime(2026, 4, 15, tzinfo=UTC),
    )

    assert context.source_type == "announcement"
    assert context.timestamp == datetime(2026, 4, 15, tzinfo=UTC)


def test_resolution_decision_builds_unresolved_decision() -> None:
    decision = ResolutionDecision(
        selected_entity_id=None,
        method=ResolutionMethod.UNRESOLVED,
        confidence=None,
        rationale="no deterministic or fuzzy candidates",
    )

    assert decision.selected_entity_id is None
    assert decision.method is ResolutionMethod.UNRESOLVED


def test_batch_resolution_job_builds_pending_state() -> None:
    job = BatchResolutionJob(
        job_id="job-1",
        reference_ids=["ref-1", "ref-2"],
        status="pending",
        created_at=datetime(2026, 4, 15, tzinfo=UTC),
        completed_at=None,
    )

    assert job.reference_ids == ["ref-1", "ref-2"]
    assert job.completed_at is None


def test_batch_resolution_job_round_trips() -> None:
    job = BatchResolutionJob(
        job_id="job-2",
        reference_ids=[],
        status="completed",
        created_at=datetime(2026, 4, 15, tzinfo=UTC),
        completed_at=datetime(2026, 4, 16, tzinfo=UTC),
    )

    restored = BatchResolutionJob.model_validate(job.model_dump(mode="json"))

    assert restored == job
