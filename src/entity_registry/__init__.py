"""Core public exports for the entity-registry package."""

from __future__ import annotations

from collections.abc import Sequence
from uuid import uuid4

from entity_registry.core import (
    AliasType,
    DecisionType,
    EntityStatus,
    EntityType,
    FinalStatus,
    ResolutionMethod,
    generate_stock_entity_id,
    validate_entity_id,
)
from entity_registry.aliases import (
    AliasManager,
    generate_aliases_from_stock_basic,
    lookup_alias as _runtime_lookup_alias,
)
from entity_registry.contracts import (
    CANONICAL_ID_RULE_VERSION,
    ContractBaseModel,
    ContractCanonicalEntity,
    ContractEntityAlias,
    ContractEntityReference,
    ContractResolutionCase,
    EntityResolutionDecision,
    current_canonical_id_rule_version,
    to_contract_canonical_entity,
    to_contract_entity_alias,
    to_contract_entity_reference,
    to_contract_resolution_case,
)
from entity_registry.batch import (
    BatchCandidateGroup,
    BatchReferenceInput,
    BatchResolutionOutcome,
    BatchResolutionReport,
    batch_resolve_with_report,
    batch_resolve_with_report as _runtime_batch_resolve_with_report,
)
from entity_registry.fuzzy import (
    FuzzyCandidate,
    FuzzyMatcher,
    FuzzyMatcherUnavailable,
    NullFuzzyMatcher,
    SimpleFuzzyMatcher,
    SplinkFuzzyMatcher,
)
from entity_registry.llm_client import (
    CallableReasonerRuntimeClient,
    LLMDisambiguationCandidate,
    LLMDisambiguationRequest,
    LLMDisambiguationResponse,
    ReasonerRuntimeClient,
    build_disambiguation_request,
)
from entity_registry.init import (
    DataPlatformStockBasicReader,
    FileStockBasicSnapshotReader,
    InitializationError,
    InitializationResult,
    RepositoryNotConfiguredError,
    StockBasicRecord,
    StockBasicSnapshotReader,
    configure_default_in_memory_audit_repositories,
    configure_default_repositories,
    detect_cross_listing_groups,
    get_default_alias_repository,
    get_default_entity_repository,
    get_default_reasoner_client,
    get_default_repositories,
    initialize_from_stock_basic,
    initialize_from_stock_basic_into,
    load_stock_basic_records,
    reset_default_repositories,
)
from entity_registry.ner import (
    ExtractedMention,
    HanLPNERExtractor,
    NERExtractor,
    NullNERExtractor,
)
from entity_registry.profile import (
    get_entity_profile as _runtime_get_entity_profile,
)
from entity_registry.references import (
    EntityReference as _RuntimeEntityReference,
    ResolutionCase as _RuntimeResolutionCase,
    _coerce_unresolved_reference,
)
from entity_registry.review import (
    ManualReviewDecision,
    ResolutionAuditPayload,
    ReviewAuditWriter,
    ReviewNotFoundError,
    ReviewStateError,
    UnresolvedQueueItem,
    claim_review_item,
    enqueue_batch_manual_review,
    enqueue_unresolved_reference,
    get_resolution_audit_payload,
    submit_manual_review_decision,
)
from entity_registry.resolution import (
    DeterministicMatcher,
    ResolutionAuditRepositoryRequiredError,
    resolve_mention_with_repositories as _runtime_resolve_mention_with_repositories,
)
from entity_registry.resolution_types import (
    BatchResolutionJob,
    MentionCandidateSet,
    MentionResolutionResult as _RuntimeMentionResolutionResult,
    ResolutionContext,
    ResolutionDecision,
)
from entity_registry.storage import (
    InMemoryReviewRepository,
    InMemoryResolutionCaseRepository,
    ReferenceRepository,
    ReviewRepository,
    ResolutionCaseRepository,
)

__version__ = "0.1.0"

CanonicalEntity = ContractCanonicalEntity
EntityAlias = ContractEntityAlias
EntityReference = ContractEntityReference
ResolutionCase = ContractResolutionCase


def lookup_alias(alias_text: str) -> ContractCanonicalEntity | None:
    """Return the contract canonical entity for one unambiguous alias hit."""

    entity = _runtime_lookup_alias(alias_text)
    if entity is None:
        return None
    return to_contract_canonical_entity(entity)


def register_unresolved_reference(
    reference: _RuntimeEntityReference | dict[str, object],
) -> ContractResolutionCase:
    """Register an unresolved reference and return a contract audit payload."""

    from entity_registry.init import (
        RepositoryNotConfiguredError,
        _get_default_repository_context,
    )

    repository_context = _get_default_repository_context()
    if repository_context.reference_repo is None:
        raise RepositoryNotConfiguredError(
            "reference audit repository is not configured; "
            "call configure_default_repositories(..., reference_repo=...) before "
            "registering unresolved references, or use "
            "configure_default_in_memory_audit_repositories() for tests/local workflows",
        )
    if repository_context.case_repo is None:
        raise RepositoryNotConfiguredError(
            "resolution case audit repository is not configured; "
            "call configure_default_repositories(..., case_repo=...) before "
            "registering unresolved references, or use "
            "configure_default_in_memory_audit_repositories() for tests/local workflows",
        )

    unresolved_reference = _coerce_unresolved_reference(reference)
    case = _RuntimeResolutionCase(
        case_id=_new_public_case_id(),
        reference_id=unresolved_reference.reference_id,
        candidate_entity_ids=[],
        selected_entity_id=None,
        decision_type=DecisionType.AUTO,
        decision_rationale="registered unresolved reference",
        created_at=unresolved_reference.created_at,
    )
    persisted_case = _save_public_resolution_audit(
        unresolved_reference,
        case,
        reference_repo=repository_context.reference_repo,
        case_repo=repository_context.case_repo,
    )
    return to_contract_resolution_case(
        persisted_case,
        input_alias=unresolved_reference.raw_mention_text,
        candidate_entities=[],
        decision=EntityResolutionDecision.UNRESOLVED,
        confidence=0.0,
    )


def resolve_mention(
    raw_mention_text: str,
    context: ResolutionContext | dict[str, object] | None = None,
) -> ContractResolutionCase:
    """Resolve one mention and return the contract resolution case."""

    from entity_registry.init import (
        RepositoryNotConfiguredError,
        _get_default_repository_context,
    )

    repository_context = _get_default_repository_context()
    if (
        repository_context.reference_repo is None
        or repository_context.case_repo is None
    ):
        raise RepositoryNotConfiguredError(
            "resolution audit repositories are not configured; "
            "call configure_default_repositories(..., reference_repo=..., "
            "case_repo=...) before using public resolution APIs, or use "
            "configure_default_in_memory_audit_repositories() for tests/local workflows",
        )

    reference_id = _new_public_reference_id()
    result = _runtime_resolve_mention_with_repositories(
        raw_mention_text,
        context,
        entity_repo=repository_context.entity_repo,
        alias_repo=repository_context.alias_repo,
        reference_repo=repository_context.reference_repo,
        case_repo=repository_context.case_repo,
        fuzzy_matcher=repository_context.fuzzy_matcher,
        ner_extractor=repository_context.ner_extractor,
        reasoner_client=repository_context.reasoner_client,
        existing_reference_id=reference_id,
    )
    return _contract_case_for_reference(
        reference_id,
        raw_mention_text,
        result,
        entity_repo=repository_context.entity_repo,
        case_repo=repository_context.case_repo,
    )


def batch_resolve(
    references: Sequence[_RuntimeEntityReference | dict[str, object] | str],
) -> list[ContractResolutionCase]:
    """Resolve a batch of mentions and return contract resolution cases."""

    from entity_registry.init import _get_default_repository_context

    report = _runtime_batch_resolve_with_report(references)
    repository_context = _get_default_repository_context()
    if repository_context.case_repo is None:
        raise RuntimeError("resolution audit repository context disappeared")

    return [
        _contract_case_for_reference(
            outcome.source_reference_id,
            outcome.result.raw_mention_text,
            outcome.result,
            entity_repo=repository_context.entity_repo,
            case_repo=repository_context.case_repo,
        )
        for outcome in report.outcomes
        if outcome.source_reference_id is not None
    ]


def get_entity_profile(canonical_entity_id: str) -> dict[str, object]:
    """Return a contract-shaped entity profile payload."""

    profile = _runtime_get_entity_profile(canonical_entity_id)
    return {
        "canonical_entity": to_contract_canonical_entity(profile.canonical_entity),
        "aliases": [
            to_contract_entity_alias(alias)
            for alias in profile.aliases
        ],
        "cross_listing_group": profile.cross_listing_group,
        "cross_listing_entity_ids": list(profile.cross_listing_entity_ids),
    }


def _contract_case_for_reference(
    reference_id: str | None,
    raw_mention_text: str,
    result: _RuntimeMentionResolutionResult,
    *,
    entity_repo: object,
    case_repo: ResolutionCaseRepository,
) -> ContractResolutionCase:
    if reference_id is None:
        raise RuntimeError("contract resolution payload requires a source reference ID")

    case = _latest_case_for_reference(case_repo, reference_id)
    candidate_entities = _candidate_entities_for_case(case, entity_repo)
    return to_contract_resolution_case(
        case,
        input_alias=raw_mention_text,
        candidate_entities=candidate_entities,
        decision=_contract_decision_for(case, result),
        confidence=result.resolution_confidence,
    )


def _latest_case_for_reference(
    case_repo: ResolutionCaseRepository,
    reference_id: str,
) -> _RuntimeResolutionCase:
    cases = case_repo.find_by_reference(reference_id)
    if not cases:
        raise RuntimeError(f"missing resolution case for reference {reference_id}")
    return max(cases, key=lambda case: case.created_at)


def _candidate_entities_for_case(
    case: _RuntimeResolutionCase,
    entity_repo: object,
) -> list[object]:
    entities = []
    for entity_id in case.candidate_entity_ids:
        entity = entity_repo.get(entity_id)
        if entity is not None:
            entities.append(entity)
    return entities


def _contract_decision_for(
    case: _RuntimeResolutionCase,
    result: _RuntimeMentionResolutionResult,
) -> EntityResolutionDecision:
    if result.resolved_entity_id is not None:
        return EntityResolutionDecision.MATCHED
    if case.candidate_entity_ids:
        return EntityResolutionDecision.AMBIGUOUS
    return EntityResolutionDecision.UNRESOLVED


def _save_public_resolution_audit(
    reference: _RuntimeEntityReference,
    case: _RuntimeResolutionCase,
    *,
    reference_repo: ReferenceRepository,
    case_repo: ResolutionCaseRepository,
) -> _RuntimeResolutionCase:
    save_resolution = getattr(reference_repo, "save_resolution", None)
    if not callable(save_resolution):
        raise ResolutionAuditRepositoryRequiredError(
            "resolution audit writes require a native save_resolution(reference, case) "
            "unit of work; separate reference/case writes are not supported",
        )

    previous_reference = reference_repo.get(reference.reference_id)
    try:
        save_resolution(reference, case)
    except Exception:
        _restore_reference_after_audit_failure(
            reference_repo,
            reference.reference_id,
            previous_reference,
        )
        raise

    persisted_case = case_repo.get(case.case_id)
    if persisted_case is None:
        _restore_reference_after_audit_failure(
            reference_repo,
            reference.reference_id,
            previous_reference,
        )
        raise RuntimeError(
            "resolution audit repository did not persist the unresolved "
            f"resolution case {case.case_id}",
        )
    return persisted_case


def _restore_reference_after_audit_failure(
    reference_repo: ReferenceRepository,
    reference_id: str,
    previous_reference: _RuntimeEntityReference | None,
) -> None:
    if previous_reference is None:
        reference_repo.delete(reference_id)
        return
    reference_repo.save(previous_reference)


def _new_public_reference_id() -> str:
    return f"REF_{uuid4().hex}"


def _new_public_case_id() -> str:
    return f"CASE_{uuid4().hex}"


__all__ = [
    "__version__",
    "AliasType",
    "AliasManager",
    "BatchCandidateGroup",
    "BatchReferenceInput",
    "BatchResolutionJob",
    "BatchResolutionOutcome",
    "BatchResolutionReport",
    "CallableReasonerRuntimeClient",
    "CANONICAL_ID_RULE_VERSION",
    "CanonicalEntity",
    "ContractBaseModel",
    "ContractCanonicalEntity",
    "ContractEntityAlias",
    "ContractEntityReference",
    "ContractResolutionCase",
    "DataPlatformStockBasicReader",
    "DecisionType",
    "DeterministicMatcher",
    "EntityAlias",
    "EntityReference",
    "EntityResolutionDecision",
    "EntityStatus",
    "EntityType",
    "ExtractedMention",
    "FileStockBasicSnapshotReader",
    "FinalStatus",
    "FuzzyCandidate",
    "FuzzyMatcher",
    "FuzzyMatcherUnavailable",
    "HanLPNERExtractor",
    "InMemoryReviewRepository",
    "InMemoryResolutionCaseRepository",
    "InitializationError",
    "InitializationResult",
    "LLMDisambiguationCandidate",
    "LLMDisambiguationRequest",
    "LLMDisambiguationResponse",
    "ManualReviewDecision",
    "MentionCandidateSet",
    "NERExtractor",
    "NullFuzzyMatcher",
    "NullNERExtractor",
    "ReasonerRuntimeClient",
    "RepositoryNotConfiguredError",
    "ResolutionAuditPayload",
    "ResolutionCase",
    "ResolutionCaseRepository",
    "ResolutionContext",
    "ResolutionDecision",
    "ResolutionMethod",
    "ReviewAuditWriter",
    "ReviewNotFoundError",
    "ReviewRepository",
    "ReviewStateError",
    "SimpleFuzzyMatcher",
    "StockBasicRecord",
    "StockBasicSnapshotReader",
    "SplinkFuzzyMatcher",
    "UnresolvedQueueItem",
    "batch_resolve",
    "batch_resolve_with_report",
    "build_disambiguation_request",
    "claim_review_item",
    "configure_default_in_memory_audit_repositories",
    "configure_default_repositories",
    "current_canonical_id_rule_version",
    "detect_cross_listing_groups",
    "enqueue_batch_manual_review",
    "enqueue_unresolved_reference",
    "generate_aliases_from_stock_basic",
    "generate_stock_entity_id",
    "get_default_alias_repository",
    "get_default_entity_repository",
    "get_default_reasoner_client",
    "get_default_repositories",
    "get_entity_profile",
    "get_resolution_audit_payload",
    "initialize_from_stock_basic",
    "initialize_from_stock_basic_into",
    "load_stock_basic_records",
    "lookup_alias",
    "register_unresolved_reference",
    "reset_default_repositories",
    "resolve_mention",
    "submit_manual_review_decision",
    "to_contract_canonical_entity",
    "to_contract_entity_alias",
    "to_contract_entity_reference",
    "to_contract_resolution_case",
    "validate_entity_id",
]
