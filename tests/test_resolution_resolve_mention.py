import inspect
from collections.abc import Iterator
from pathlib import Path

import pytest
from pydantic import ValidationError

import entity_registry
from entity_registry.core import (
    AliasType,
    CanonicalEntity,
    DecisionType,
    EntityAlias,
    EntityStatus,
    EntityType,
    ResolutionMethod,
)
from entity_registry.init import (
    FileStockBasicSnapshotReader,
    initialize_from_stock_basic_into,
)
from entity_registry.references import EntityReference
from entity_registry.resolution import (
    resolve_mention,
    resolve_mention_with_repositories,
)
from entity_registry.resolution_types import MentionResolutionResult, ResolutionContext
from entity_registry.storage import (
    InMemoryAliasRepository,
    InMemoryEntityRepository,
    InMemoryReferenceRepository,
    InMemoryResolutionCaseRepository,
)


FIXTURE_PATH = Path("tests/fixtures/stock_basic_sample.json")


@pytest.fixture(autouse=True)
def reset_public_repositories() -> Iterator[None]:
    entity_registry.reset_default_repositories()
    yield
    entity_registry.reset_default_repositories()


def initialized_resolution_repositories() -> tuple[
    InMemoryEntityRepository,
    InMemoryAliasRepository,
    InMemoryReferenceRepository,
    InMemoryResolutionCaseRepository,
]:
    entity_repo = InMemoryEntityRepository()
    alias_repo = InMemoryAliasRepository()
    reference_repo = InMemoryReferenceRepository()
    case_repo = InMemoryResolutionCaseRepository()
    result = initialize_from_stock_basic_into(
        str(FIXTURE_PATH),
        entity_repo,
        alias_repo,
        stock_basic_reader=FileStockBasicSnapshotReader(),
    )
    assert result.errors == []
    return entity_repo, alias_repo, reference_repo, case_repo


def saved_references(
    reference_repo: InMemoryReferenceRepository,
) -> list[EntityReference]:
    return list(reference_repo._references.values())


def test_resolve_mention_public_signature_matches_contract() -> None:
    signature = inspect.signature(resolve_mention)

    assert list(signature.parameters) == ["raw_mention_text", "context"]
    assert signature.parameters["context"].default is None


def test_package_exports_resolution_public_api() -> None:
    assert entity_registry.resolve_mention is resolve_mention
    assert entity_registry.MentionResolutionResult is MentionResolutionResult


def test_mention_resolution_result_json_shape_is_stable() -> None:
    result = MentionResolutionResult(
        raw_mention_text="贵州茅台",
        resolved_entity_id="ENT_STOCK_600519.SH",
        resolution_method=ResolutionMethod.DETERMINISTIC,
        resolution_confidence=1.0,
    )

    assert result.model_dump(mode="json") == {
        "raw_mention_text": "贵州茅台",
        "resolved_entity_id": "ENT_STOCK_600519.SH",
        "resolution_method": "deterministic",
        "resolution_confidence": 1.0,
    }


def test_mention_resolution_result_rejects_null_entity_with_resolved_method() -> None:
    with pytest.raises(ValidationError):
        MentionResolutionResult(
            raw_mention_text="Unknown Corp",
            resolved_entity_id=None,
            resolution_method=ResolutionMethod.DETERMINISTIC,
            resolution_confidence=None,
        )


def test_unique_deterministic_hit_returns_result_and_audit_records() -> None:
    entity_repo, alias_repo, reference_repo, case_repo = (
        initialized_resolution_repositories()
    )

    result = resolve_mention_with_repositories(
        "贵州茅台",
        {"document_id": "doc-1"},
        entity_repo=entity_repo,
        alias_repo=alias_repo,
        reference_repo=reference_repo,
        case_repo=case_repo,
    )

    references = saved_references(reference_repo)
    cases = case_repo.find_by_reference(references[0].reference_id)
    assert result == MentionResolutionResult(
        raw_mention_text="贵州茅台",
        resolved_entity_id="ENT_STOCK_600519.SH",
        resolution_method=ResolutionMethod.DETERMINISTIC,
        resolution_confidence=1.0,
    )
    assert len(references) == 1
    assert references[0].resolved_entity_id == "ENT_STOCK_600519.SH"
    assert references[0].source_context == {"document_id": "doc-1"}
    assert len(cases) == 1
    assert cases[0].decision_type is DecisionType.AUTO
    assert cases[0].selected_entity_id == "ENT_STOCK_600519.SH"


def test_resolve_mention_audits_same_candidate_snapshot_used_for_decision() -> None:
    entity_repo = InMemoryEntityRepository()
    first_entity = make_entity("ENT_STOCK_FIRST.SZ")
    second_entity = make_entity("ENT_STOCK_SECOND.SZ")
    entity_repo.save(first_entity)
    entity_repo.save(second_entity)
    alias_repo = FlappingAliasRepository(
        [
            make_alias(first_entity.canonical_entity_id, "flip"),
            make_alias(second_entity.canonical_entity_id, "flip"),
        ]
    )
    reference_repo = InMemoryReferenceRepository()
    case_repo = InMemoryResolutionCaseRepository()

    result = resolve_mention_with_repositories(
        "flip",
        None,
        entity_repo=entity_repo,
        alias_repo=alias_repo,  # type: ignore[arg-type]
        reference_repo=reference_repo,
        case_repo=case_repo,
    )

    references = saved_references(reference_repo)
    cases = case_repo.find_by_reference(references[0].reference_id)
    assert alias_repo.find_by_text_calls == 1
    assert result.resolved_entity_id == "ENT_STOCK_FIRST.SZ"
    assert cases[0].selected_entity_id == "ENT_STOCK_FIRST.SZ"
    assert cases[0].candidate_entity_ids == ["ENT_STOCK_FIRST.SZ"]


def test_unique_code_hit_returns_deterministic_result() -> None:
    entity_repo, alias_repo, reference_repo, case_repo = (
        initialized_resolution_repositories()
    )

    result = resolve_mention_with_repositories(
        "600519",
        None,
        entity_repo=entity_repo,
        alias_repo=alias_repo,
        reference_repo=reference_repo,
        case_repo=case_repo,
    )

    assert result.resolved_entity_id == "ENT_STOCK_600519.SH"
    assert result.resolution_method is ResolutionMethod.DETERMINISTIC
    assert result.resolution_confidence == 1.0


def test_missing_mention_returns_unresolved_and_writes_audit_records() -> None:
    entity_repo, alias_repo, reference_repo, case_repo = (
        initialized_resolution_repositories()
    )

    result = resolve_mention_with_repositories(
        "不存在的公司",
        None,
        entity_repo=entity_repo,
        alias_repo=alias_repo,
        reference_repo=reference_repo,
        case_repo=case_repo,
    )

    unresolved = reference_repo.find_unresolved()
    cases = case_repo.find_by_reference(unresolved[0].reference_id)
    assert result.model_dump(mode="json") == {
        "raw_mention_text": "不存在的公司",
        "resolved_entity_id": None,
        "resolution_method": "unresolved",
        "resolution_confidence": None,
    }
    assert len(unresolved) == 1
    assert unresolved[0].resolution_method is ResolutionMethod.UNRESOLVED
    assert len(cases) == 1
    assert cases[0].candidate_entity_ids == []
    assert cases[0].decision_type is DecisionType.AUTO
    assert cases[0].selected_entity_id is None


def test_a_h_shared_short_name_returns_unresolved_with_candidate_case() -> None:
    entity_repo, alias_repo, reference_repo, case_repo = (
        initialized_resolution_repositories()
    )

    result = resolve_mention_with_repositories(
        "宁德时代",
        None,
        entity_repo=entity_repo,
        alias_repo=alias_repo,
        reference_repo=reference_repo,
        case_repo=case_repo,
    )

    unresolved = reference_repo.find_unresolved()
    cases = case_repo.find_by_reference(unresolved[0].reference_id)
    assert result.resolved_entity_id is None
    assert result.resolution_method is ResolutionMethod.UNRESOLVED
    assert result.resolution_confidence is None
    assert len(cases) == 1
    assert set(cases[0].candidate_entity_ids) == {
        "ENT_STOCK_300750.SZ",
        "ENT_STOCK_03750.HK",
    }
    assert cases[0].selected_entity_id is None
    assert cases[0].decision_type is DecisionType.MANUAL_REVIEW


def test_public_resolve_mention_uses_configured_default_repositories() -> None:
    entity_repo, alias_repo, reference_repo, case_repo = (
        initialized_resolution_repositories()
    )
    entity_registry.configure_default_repositories(
        entity_repo,
        alias_repo,
        reference_repo=reference_repo,
        case_repo=case_repo,
    )

    result = entity_registry.resolve_mention(
        "贵州茅台",
        ResolutionContext(
            raw_mention_text="贵州茅台",
            document_context="贵州茅台发布公告",
            source_type="announcement",
        ),
    )

    references = saved_references(reference_repo)
    assert result.resolved_entity_id == "ENT_STOCK_600519.SH"
    assert len(references) == 1
    assert references[0].source_context["source_type"] == "announcement"


def test_resolved_case_write_failure_preserves_reference_retry_outbox() -> None:
    entity_repo, alias_repo, _reference_repo, _case_repo = (
        initialized_resolution_repositories()
    )
    reference_repo = DeleteForbiddenReferenceRepository()

    with pytest.raises(RuntimeError, match="case write failed"):
        resolve_mention_with_repositories(
            "贵州茅台",
            None,
            entity_repo=entity_repo,
            alias_repo=alias_repo,
            reference_repo=reference_repo,
            case_repo=FailingResolutionCaseRepository(),
        )

    references = saved_references(reference_repo)
    assert len(references) == 1
    assert reference_repo.delete_calls == 0
    assert references[0].resolved_entity_id == "ENT_STOCK_600519.SH"
    retry = references[0].source_context["resolution_audit_outbox"]
    assert retry["status"] == "resolution_case_write_failed"
    assert retry["case"]["reference_id"] == references[0].reference_id
    assert retry["case"]["selected_entity_id"] == "ENT_STOCK_600519.SH"
    assert retry["error_type"] == "RuntimeError"
    assert retry["error_message"] == "case write failed"


def test_unresolved_case_write_failure_preserves_reference_retry_outbox() -> None:
    entity_repo, alias_repo, _reference_repo, _case_repo = (
        initialized_resolution_repositories()
    )
    reference_repo = DeleteForbiddenReferenceRepository()

    with pytest.raises(RuntimeError, match="case write failed"):
        resolve_mention_with_repositories(
            "不存在的公司",
            None,
            entity_repo=entity_repo,
            alias_repo=alias_repo,
            reference_repo=reference_repo,
            case_repo=FailingResolutionCaseRepository(),
        )

    references = saved_references(reference_repo)
    unresolved = reference_repo.find_unresolved()
    assert len(references) == 1
    assert unresolved == references
    assert reference_repo.delete_calls == 0
    retry = references[0].source_context["resolution_audit_outbox"]
    assert retry["status"] == "resolution_case_write_failed"
    assert retry["case"]["reference_id"] == references[0].reference_id
    assert retry["case"]["selected_entity_id"] is None
    assert retry["error_type"] == "RuntimeError"


def test_resolution_audit_uses_native_save_resolution_boundary() -> None:
    entity_repo, alias_repo, _reference_repo, _case_repo = (
        initialized_resolution_repositories()
    )
    reference_repo = NativeResolutionAuditReferenceRepository()

    result = resolve_mention_with_repositories(
        "贵州茅台",
        None,
        entity_repo=entity_repo,
        alias_repo=alias_repo,
        reference_repo=reference_repo,
        case_repo=UnexpectedResolutionCaseRepository(),
    )

    references = saved_references(reference_repo)
    assert result.resolved_entity_id == "ENT_STOCK_600519.SH"
    assert len(references) == 1
    assert len(reference_repo.saved_cases) == 1
    assert reference_repo.saved_cases[0].reference_id == references[0].reference_id


def test_resolution_module_has_no_provider_or_later_stage_imports() -> None:
    text = Path("src/entity_registry/resolution.py").read_text(encoding="utf-8")

    for forbidden in ("openai", "anthropic", "google.generativeai", "splink", "hanlp"):
        assert forbidden not in text


def make_entity(entity_id: str) -> CanonicalEntity:
    return CanonicalEntity(
        canonical_entity_id=entity_id,
        entity_type=EntityType.STOCK,
        display_name=entity_id,
        status=EntityStatus.ACTIVE,
        anchor_code=entity_id.removeprefix("ENT_STOCK_"),
        cross_listing_group=None,
    )


def make_alias(entity_id: str, alias_text: str) -> EntityAlias:
    return EntityAlias(
        canonical_entity_id=entity_id,
        alias_text=alias_text,
        alias_type=AliasType.SHORT_NAME,
        confidence=1.0,
        source="unit-test",
        is_primary=True,
    )


class FlappingAliasRepository:
    def __init__(self, aliases_by_call: list[EntityAlias]) -> None:
        self.aliases_by_call = aliases_by_call
        self.find_by_text_calls = 0

    def find_by_text(self, alias_text: str) -> list[EntityAlias]:
        index = min(self.find_by_text_calls, len(self.aliases_by_call) - 1)
        self.find_by_text_calls += 1
        return [self.aliases_by_call[index]]

    def find_by_entity(self, entity_id: str) -> list[EntityAlias]:
        return [
            alias
            for alias in self.aliases_by_call
            if alias.canonical_entity_id == entity_id
        ]

    def save(self, alias: EntityAlias) -> None:
        raise NotImplementedError

    def save_if_absent(self, alias: EntityAlias) -> bool:
        raise NotImplementedError

    def save_batch(self, aliases: list[EntityAlias]) -> None:
        raise NotImplementedError

    def save_batch_if_absent(self, aliases: list[EntityAlias]) -> int:
        raise NotImplementedError


class FailingResolutionCaseRepository(InMemoryResolutionCaseRepository):
    def save(self, case: entity_registry.ResolutionCase) -> None:
        raise RuntimeError("case write failed")


class UnexpectedResolutionCaseRepository(InMemoryResolutionCaseRepository):
    def save(self, case: entity_registry.ResolutionCase) -> None:
        raise AssertionError("native audit unit of work should save cases")


class DeleteForbiddenReferenceRepository(InMemoryReferenceRepository):
    def __init__(self) -> None:
        super().__init__()
        self.delete_calls = 0

    def delete(self, reference_id: str) -> None:
        self.delete_calls += 1
        raise AssertionError("resolution audit must not delete references")


class NativeResolutionAuditReferenceRepository(InMemoryReferenceRepository):
    def __init__(self) -> None:
        super().__init__()
        self.saved_cases: list[entity_registry.ResolutionCase] = []

    def save_resolution(
        self,
        reference: EntityReference,
        case: entity_registry.ResolutionCase,
    ) -> None:
        self.save(reference)
        self.saved_cases.append(case)
