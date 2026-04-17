import inspect
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

import entity_registry
import entity_registry.batch as batch_module
from entity_registry.batch import (
    BatchCandidateGroup,
    BatchReferenceInput,
    BatchResolutionOutcome,
    BatchResolutionReport,
    batch_resolve,
    cluster_unresolved_references,
    collect_unresolved_references,
    run_batch_resolution_job,
)
from entity_registry.core import AliasType, FinalStatus, ResolutionMethod
from entity_registry.fuzzy import FuzzyCandidate
from entity_registry.init import (
    FileStockBasicSnapshotReader,
    initialize_from_stock_basic_into,
)
from entity_registry.llm_client import (
    LLMDisambiguationRequest,
    LLMDisambiguationResponse,
)
from entity_registry.ner import ExtractedMention
from entity_registry.references import EntityReference
from entity_registry.resolution_types import BatchResolutionJob, MentionResolutionResult
from entity_registry.storage import (
    InMemoryAliasRepository,
    InMemoryEntityRepository,
    InMemoryReferenceRepository,
    InMemoryResolutionAuditReferenceRepository,
    InMemoryResolutionCaseRepository,
)


FIXTURE_PATH = Path("tests/fixtures/stock_basic_sample.json")


@pytest.fixture(autouse=True)
def reset_public_repositories() -> Iterator[None]:
    entity_registry.reset_default_repositories()
    yield
    entity_registry.reset_default_repositories()


def test_batch_resolve_public_signature_and_exports() -> None:
    signature = inspect.signature(batch_resolve)

    assert list(signature.parameters) == ["references"]
    assert entity_registry.batch_resolve is batch_resolve
    assert entity_registry.BatchReferenceInput is BatchReferenceInput
    assert entity_registry.BatchCandidateGroup is BatchCandidateGroup
    assert entity_registry.BatchResolutionOutcome is BatchResolutionOutcome
    assert entity_registry.BatchResolutionReport is BatchResolutionReport


def test_batch_resolve_normalizes_supported_inputs_and_preserves_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def resolver(
        raw_mention_text: str,
        context: object = None,
    ) -> MentionResolutionResult:
        assert isinstance(context, dict)
        calls.append((raw_mention_text, context))
        return resolved_result(raw_mention_text)

    monkeypatch.setattr(batch_module, "resolve_mention", resolver)
    reference = make_reference(
        "ref-entity",
        "EntityRef Co",
        {"source": "entity-reference"},
    )

    results = batch_resolve(
        [
            reference,
            {
                "reference_id": "ref-dict",
                "raw_mention_text": "Dict Co",
                "source_context": {"document_id": "doc-1", "offset": 9},
            },
            "Bare Co",
        ]
    )

    assert [result.raw_mention_text for result in results] == [
        "EntityRef Co",
        "Dict Co",
        "Bare Co",
    ]
    assert calls == [
        ("EntityRef Co", {"source": "entity-reference"}),
        ("Dict Co", {"document_id": "doc-1", "offset": 9}),
        ("Bare Co", {}),
    ]


def test_collect_unresolved_references_filters_sorts_and_limits() -> None:
    repository = InMemoryReferenceRepository()
    newer = make_reference(
        "ref-newer",
        "Newer",
        created_at=datetime(2026, 4, 16, tzinfo=UTC),
    )
    older = make_reference(
        "ref-older",
        "Older",
        created_at=datetime(2026, 4, 15, tzinfo=UTC),
    )
    resolved = make_reference(
        "ref-resolved",
        "Resolved",
        resolved_entity_id="ENT_STOCK_600519.SH",
    )

    repository.save(newer)
    repository.save(resolved)
    repository.save(older)

    assert collect_unresolved_references(repository) == [older, newer]
    assert collect_unresolved_references(repository, limit=1) == [older]


def test_cluster_unresolved_references_groups_by_normalized_mention_and_candidates() -> None:
    first = make_reference("ref-1", " CATL ", {"market": "A-share"})
    second = make_reference("ref-2", "catl", {"market": "HK"})
    third = make_reference("ref-3", "贵州茅台")
    matcher = RecordingFuzzyMatcher(
        {
            " CATL ": [
                make_candidate("ENT_STOCK_300750.SZ", score=0.90, alias_text="CATL"),
                make_candidate("ENT_STOCK_03750.HK", score=0.91, alias_text="CATL"),
            ],
            "catl": [
                make_candidate("ENT_STOCK_03750.HK", score=0.89, alias_text="CATL"),
                make_candidate("ENT_STOCK_300750.SZ", score=0.88, alias_text="CATL"),
            ],
            "贵州茅台": [
                make_candidate(
                    "ENT_STOCK_600519.SH",
                    score=0.97,
                    alias_text="贵州茅台",
                )
            ],
        }
    )

    groups = cluster_unresolved_references(
        [third, first, second],
        fuzzy_matcher=matcher,
        limit=5,
    )

    catl_group = next(group for group in groups if group.normalized_mention == "catl")
    assert catl_group.reference_ids == ["ref-1", "ref-2"]
    assert catl_group.raw_mentions == [" CATL ", "catl"]
    assert catl_group.candidate_entity_ids == [
        "ENT_STOCK_03750.HK",
        "ENT_STOCK_300750.SZ",
    ]
    assert catl_group.max_score == 0.91
    assert len(groups) == 2
    assert matcher.calls == [
        ("贵州茅台", {"limit": 5, "context": {}}),
        (" CATL ", {"limit": 5, "context": {"market": "A-share"}}),
        ("catl", {"limit": 5, "context": {"market": "HK"}}),
    ]


def test_run_batch_resolution_job_delegates_and_dedupes_duplicate_reference_id() -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def resolver(
        raw_mention_text: str,
        context: object = None,
    ) -> MentionResolutionResult:
        assert isinstance(context, dict)
        calls.append((raw_mention_text, context))
        return resolved_result(raw_mention_text)

    job = BatchResolutionJob(job_id="job-dedupe", reference_ids=[], status="pending")
    report = run_batch_resolution_job(
        job,
        [
            {
                "reference_id": "ref-1",
                "raw_mention_text": "贵州茅台",
                "source_context": {"document_id": "doc-1"},
            },
            {
                "reference_id": "ref-1",
                "raw_mention_text": "贵州茅台",
                "source_context": {"document_id": "doc-1"},
            },
        ],
        resolver=resolver,
    )

    assert calls == [("贵州茅台", {"document_id": "doc-1"})]
    assert [outcome.result for outcome in report.outcomes] == [
        resolved_result("贵州茅台"),
        resolved_result("贵州茅台"),
    ]
    assert report.resolved_reference_ids == ["ref-1"]
    assert report.job.reference_ids == ["ref-1"]
    assert report.manual_review_reference_ids == []
    assert report.job.status == "completed"
    assert report.job.completed_at is not None


def test_run_batch_resolution_job_keeps_completed_outcomes_when_one_item_fails() -> None:
    def resolver(
        raw_mention_text: str,
        context: object = None,
    ) -> MentionResolutionResult:
        if raw_mention_text == "Broken":
            raise RuntimeError("resolver exploded")
        return resolved_result(raw_mention_text)

    job = BatchResolutionJob(job_id="job-fail", reference_ids=[], status="pending")
    report = run_batch_resolution_job(
        job,
        [
            {"reference_id": "ref-ok", "raw_mention_text": "贵州茅台"},
            {"reference_id": "ref-bad", "raw_mention_text": "Broken"},
        ],
        resolver=resolver,
    )

    assert report.job.status == "failed"
    assert report.job.error_summary is not None
    assert "ref-bad" in report.errors[0]
    assert report.outcomes[0].final_status is FinalStatus.RESOLVED
    assert report.outcomes[1].final_status is FinalStatus.UNRESOLVED
    assert report.outcomes[1].result.resolution_method is ResolutionMethod.UNRESOLVED
    assert report.resolved_reference_ids == ["ref-ok"]
    assert report.unresolved_reference_ids == ["ref-bad"]
    assert report.manual_review_reference_ids == ["ref-bad"]


def test_public_batch_resolve_uses_configured_ner_fuzzy_reasoner_and_audit() -> None:
    entity_repo, alias_repo = initialized_repositories()
    case_repo = InMemoryResolutionCaseRepository()
    reference_repo = InMemoryResolutionAuditReferenceRepository(case_repo)
    fuzzy_matcher = RecordingFuzzyMatcher(
        {
            "宁德时代新能源": [
                make_candidate(
                    "ENT_STOCK_03750.HK",
                    score=0.91,
                    alias_text="宁德时代新能源科技股份有限公司",
                ),
                make_candidate(
                    "ENT_STOCK_300750.SZ",
                    score=0.90,
                    alias_text="宁德时代新能源科技股份有限公司",
                ),
            ]
        }
    )
    ner_extractor = StaticNERExtractor("宁德时代新能源")
    reasoner_client = RecordingReasonerClient(
        LLMDisambiguationResponse(
            selected_entity_id="ENT_STOCK_03750.HK",
            confidence=0.89,
            rationale="HK market context",
        )
    )
    entity_registry.configure_default_repositories(
        entity_repo,
        alias_repo,
        reference_repo=reference_repo,
        case_repo=case_repo,
        fuzzy_matcher=fuzzy_matcher,
        ner_extractor=ner_extractor,
        reasoner_client=reasoner_client,
    )

    results = batch_resolve(
        [
            {
                "reference_id": "ref-chain",
                "raw_mention_text": "公告称宁德时代新能源获增持",
                "source_context": {"market": "HK"},
            }
        ]
    )

    assert results[0].model_dump(mode="json") == {
        "raw_mention_text": "公告称宁德时代新能源获增持",
        "resolved_entity_id": "ENT_STOCK_03750.HK",
        "resolution_method": "llm",
        "resolution_confidence": 0.89,
    }
    assert ner_extractor.calls == ["公告称宁德时代新能源获增持"]
    assert fuzzy_matcher.calls == [
        ("宁德时代新能源", {"limit": 10, "context": {"market": "HK"}})
    ]
    assert len(reasoner_client.calls) == 1
    assert list(reference_repo._references.values())[0].source_context == {"market": "HK"}


def test_manual_review_routing_keeps_a_h_shared_short_name_unresolved() -> None:
    entity_repo, alias_repo = initialized_repositories()
    case_repo = InMemoryResolutionCaseRepository()
    reference_repo = InMemoryResolutionAuditReferenceRepository(case_repo)
    entity_registry.configure_default_repositories(
        entity_repo,
        alias_repo,
        reference_repo=reference_repo,
        case_repo=case_repo,
    )
    source = make_reference("ref-ah", "宁德时代")
    job = BatchResolutionJob(job_id="job-ah", reference_ids=[], status="pending")

    report = run_batch_resolution_job(job, [source])

    outcome = report.outcomes[0]
    saved_reference = list(reference_repo._references.values())[0]
    case = case_repo.find_by_reference(saved_reference.reference_id)[0]
    assert outcome.result.resolved_entity_id is None
    assert outcome.result.resolution_method is ResolutionMethod.UNRESOLVED
    assert report.manual_review_reference_ids == ["ref-ah"]
    assert report.unresolved_reference_ids == ["ref-ah"]
    assert set(case.candidate_entity_ids) == {
        "ENT_STOCK_300750.SZ",
        "ENT_STOCK_03750.HK",
    }


def test_batch_report_and_job_round_trip_with_error_metadata() -> None:
    job = BatchResolutionJob(
        job_id="job-round-trip",
        reference_ids=["ref-1"],
        status="failed",
        started_at=datetime(2026, 4, 15, tzinfo=UTC),
        completed_at=datetime(2026, 4, 16, tzinfo=UTC),
        error_summary="ref-1: RuntimeError: failure",
    )
    report = BatchResolutionReport(
        job=job,
        groups=[],
        outcomes=[
            BatchResolutionOutcome(
                source_reference_id="ref-1",
                result=unresolved_result("Unknown"),
                final_status=FinalStatus.UNRESOLVED,
                error="RuntimeError: failure",
            )
        ],
        resolved_reference_ids=[],
        unresolved_reference_ids=["ref-1"],
        manual_review_reference_ids=["ref-1"],
        errors=["ref-1: RuntimeError: failure"],
    )

    restored = BatchResolutionReport.model_validate(report.model_dump(mode="json"))

    assert restored == report


def test_batch_module_has_no_provider_sdk_imports() -> None:
    for path in (
        "src/entity_registry/batch.py",
        "src/entity_registry/resolution.py",
        "src/entity_registry/fuzzy.py",
    ):
        text = Path(path).read_text(encoding="utf-8")
        for forbidden in ("openai", "anthropic", "google.generativeai"):
            assert forbidden not in text


def initialized_repositories() -> tuple[
    InMemoryEntityRepository,
    InMemoryAliasRepository,
]:
    entity_repo = InMemoryEntityRepository()
    alias_repo = InMemoryAliasRepository()
    result = initialize_from_stock_basic_into(
        str(FIXTURE_PATH),
        entity_repo,
        alias_repo,
        stock_basic_reader=FileStockBasicSnapshotReader(),
    )
    assert result.errors == []
    return entity_repo, alias_repo


def make_reference(
    reference_id: str,
    raw_mention_text: str,
    source_context: dict[str, object] | None = None,
    *,
    resolved_entity_id: str | None = None,
    created_at: datetime | None = None,
) -> EntityReference:
    return EntityReference(
        reference_id=reference_id,
        raw_mention_text=raw_mention_text,
        source_context={} if source_context is None else source_context,
        resolved_entity_id=resolved_entity_id,
        resolution_method=(
            ResolutionMethod.DETERMINISTIC
            if resolved_entity_id is not None
            else ResolutionMethod.UNRESOLVED
        ),
        resolution_confidence=1.0 if resolved_entity_id is not None else None,
        created_at=created_at or datetime(2026, 4, 15, tzinfo=UTC),
    )


def make_candidate(
    entity_id: str,
    *,
    score: float,
    alias_text: str,
) -> FuzzyCandidate:
    return FuzzyCandidate(
        canonical_entity_id=entity_id,
        alias_text=alias_text,
        alias_type=AliasType.SHORT_NAME,
        score=score,
        source="unit-test",
        blocking_key=alias_text[:2],
    )


def resolved_result(raw_mention_text: str) -> MentionResolutionResult:
    return MentionResolutionResult(
        raw_mention_text=raw_mention_text,
        resolved_entity_id="ENT_STOCK_600519.SH",
        resolution_method=ResolutionMethod.DETERMINISTIC,
        resolution_confidence=1.0,
    )


def unresolved_result(raw_mention_text: str) -> MentionResolutionResult:
    return MentionResolutionResult(
        raw_mention_text=raw_mention_text,
        resolved_entity_id=None,
        resolution_method=ResolutionMethod.UNRESOLVED,
        resolution_confidence=None,
    )


class RecordingFuzzyMatcher:
    auto_resolve_score = 0.96

    def __init__(self, candidates_by_text: dict[str, list[FuzzyCandidate]]) -> None:
        self._candidates_by_text = candidates_by_text
        self.calls: list[tuple[str, dict[str, object]]] = []

    def generate_candidates(
        self,
        raw_mention_text: str,
        *,
        context: object = None,
        limit: int = 10,
    ) -> list[FuzzyCandidate]:
        self.calls.append(
            (
                raw_mention_text,
                {
                    "limit": limit,
                    "context": context if isinstance(context, dict) else {},
                },
            )
        )
        return self._candidates_by_text.get(raw_mention_text, [])[:limit]


class StaticNERExtractor:
    def __init__(self, mention_text: str) -> None:
        self._mention_text = mention_text
        self.calls: list[str] = []

    def extract_mentions(
        self,
        text: str,
        *,
        context: object = None,
    ) -> list[ExtractedMention]:
        self.calls.append(text)
        return [ExtractedMention(mention_text=self._mention_text)]


class RecordingReasonerClient:
    def __init__(self, response: LLMDisambiguationResponse) -> None:
        self._response = response
        self.calls: list[LLMDisambiguationRequest] = []

    def disambiguate(
        self,
        request: LLMDisambiguationRequest,
    ) -> LLMDisambiguationResponse:
        self.calls.append(request)
        return self._response
