"""Deterministic mention matching."""

from __future__ import annotations

import math
from typing import Protocol

from entity_registry.aliases import AliasManager, normalize_alias_text
from entity_registry.core import (
    AliasType,
    CanonicalEntity,
    DecisionType,
    EntityAlias,
    FinalStatus,
    ResolutionMethod,
    generate_stock_entity_id,
    validate_entity_id,
)
from entity_registry.fuzzy import FuzzyCandidate, FuzzyMatcher
from entity_registry.ner import NERExtractor
from entity_registry.references import (
    EntityReference,
    ResolutionCase,
    _new_case_id,
    _new_reference_id,
)
from entity_registry.resolution_types import (
    MentionCandidateSet,
    MentionResolutionResult,
    ResolutionContext,
    ResolutionDecision,
)
from entity_registry.storage import (
    AliasRepository,
    EntityRepository,
    ReferenceRepository,
    ResolutionCaseRepository,
)


class ResolutionAuditRepository(Protocol):
    """Unit-of-work contract for writing resolution audit records."""

    def save_resolution(
        self,
        reference: EntityReference,
        case: ResolutionCase,
    ) -> None: ...


class ResolutionAuditRepositoryRequiredError(RuntimeError):
    """Raised when resolution is asked to write audit records without a UoW."""


class _RepositoryResolutionAuditRepository:
    """Resolution audit unit of work over native repository contracts."""

    def __init__(
        self,
        reference_repo: ReferenceRepository,
        case_repo: ResolutionCaseRepository,
    ) -> None:
        self._reference_repo = reference_repo
        self._case_repo = case_repo

    def save_resolution(
        self,
        reference: EntityReference,
        case: ResolutionCase,
    ) -> None:
        native_save_resolution = getattr(
            self._reference_repo,
            "save_resolution",
            None,
        )
        if callable(native_save_resolution):
            native_save_resolution(reference, case)
            return

        raise ResolutionAuditRepositoryRequiredError(
            "resolution audit writes require a native save_resolution(reference, case) "
            "unit of work; separate reference/case writes are not supported",
        )


class DeterministicMatcher:
    """Repository-backed Level 1 deterministic matcher."""

    def __init__(
        self,
        entity_repo: EntityRepository,
        alias_repo: AliasRepository,
    ) -> None:
        self._entity_repo = entity_repo
        self._alias_manager = AliasManager(alias_repo)

    def exact_match(self, raw_mention_text: str) -> list[CanonicalEntity]:
        """Return existing entities for exact alias text matches."""

        aliases = self._alias_manager.lookup(raw_mention_text)
        return self._entities_for_aliases(aliases)

    def code_match(self, raw_mention_text: str) -> list[CanonicalEntity]:
        """Return existing entities for stock code aliases only."""

        aliases = [
            alias
            for alias in self._alias_manager.lookup(raw_mention_text)
            if alias.alias_type is AliasType.CODE
        ]
        return self._entities_for_aliases(aliases)

    def rule_match(self, raw_mention_text: str) -> list[CanonicalEntity]:
        """Return entities for verified ENT_* IDs or suffixed stock ts_codes."""

        normalized_text = normalize_alias_text(raw_mention_text)
        if not normalized_text:
            return []

        if validate_entity_id(normalized_text):
            entity = self._entity_repo.get(normalized_text)
            return [] if entity is None else [entity]

        if "." not in normalized_text:
            return []

        try:
            canonical_entity_id = generate_stock_entity_id(normalized_text)
        except ValueError:
            return []

        entity = self._entity_repo.get(canonical_entity_id)
        return [] if entity is None else [entity]

    def collect_candidates(
        self,
        raw_mention_text: str,
        *,
        context: ResolutionContext | dict[str, object] | None = None,
        fuzzy_matcher: FuzzyMatcher | None = None,
        ner_extractor: NERExtractor | None = None,
        limit: int = 10,
    ) -> MentionCandidateSet:
        """Collect deterministic candidates, then fuzzy candidates if needed."""

        return self.collect_candidates_with_fuzzy(
            raw_mention_text,
            context=context,
            fuzzy_matcher=fuzzy_matcher,
            ner_extractor=ner_extractor,
            limit=limit,
        )

    def collect_candidates_with_fuzzy(
        self,
        raw_mention_text: str,
        *,
        context: ResolutionContext | dict[str, object] | None = None,
        fuzzy_matcher: FuzzyMatcher | None = None,
        ner_extractor: NERExtractor | None = None,
        limit: int = 10,
    ) -> MentionCandidateSet:
        """Collect deterministic candidates, then fuzzy candidates if needed."""

        candidates = self._collect_deterministic_candidates(raw_mention_text)

        deterministic_hits = [
            entity.canonical_entity_id
            for entity in candidates
        ]
        if len(deterministic_hits) == 1:
            return MentionCandidateSet(
                raw_mention_text=raw_mention_text,
                deterministic_hits=deterministic_hits,
                fuzzy_hits=[],
                llm_required=False,
                final_status=FinalStatus.RESOLVED,
            )

        fuzzy_candidates = _generate_fuzzy_candidates(
            raw_mention_text,
            context=context,
            fuzzy_matcher=fuzzy_matcher,
            ner_extractor=ner_extractor,
            limit=limit,
        )
        fuzzy_hits = [candidate.canonical_entity_id for candidate in fuzzy_candidates]
        fuzzy_scores = {
            candidate.canonical_entity_id: candidate.score
            for candidate in fuzzy_candidates
        }
        final_status, llm_required = _candidate_status(
            deterministic_hits,
            fuzzy_hits,
            fuzzy_scores,
            auto_resolve_threshold=_auto_resolve_threshold(fuzzy_matcher),
        )

        return MentionCandidateSet(
            raw_mention_text=raw_mention_text,
            deterministic_hits=deterministic_hits,
            fuzzy_hits=fuzzy_hits,
            fuzzy_scores=fuzzy_scores,
            llm_required=llm_required,
            final_status=final_status,
        )

    def resolve(self, raw_mention_text: str) -> ResolutionDecision:
        """Return a deterministic decision without audit writes."""

        candidate_set = self.collect_candidates(raw_mention_text)
        return _decision_from_candidate_set(candidate_set)

    def resolve_candidate_set(
        self,
        candidate_set: MentionCandidateSet,
        *,
        auto_resolve_threshold: float = 0.96,
    ) -> ResolutionDecision:
        """Return a deterministic decision from a pre-collected candidate set."""

        return _decision_from_candidate_set(
            candidate_set,
            auto_resolve_threshold=auto_resolve_threshold,
        )

    def _collect_deterministic_candidates(
        self,
        raw_mention_text: str,
    ) -> list[CanonicalEntity]:
        candidates = self.rule_match(raw_mention_text)
        if candidates:
            return candidates

        aliases = self._alias_manager.lookup(raw_mention_text)
        code_aliases = [
            alias
            for alias in aliases
            if alias.alias_type is AliasType.CODE
        ]
        if code_aliases:
            return self._entities_for_aliases(code_aliases)

        return self._entities_for_aliases(aliases)

    def _entities_for_aliases(
        self,
        aliases: list[EntityAlias],
    ) -> list[CanonicalEntity]:
        entities_by_id: dict[str, CanonicalEntity] = {}
        for alias in aliases:
            if alias.canonical_entity_id in entities_by_id:
                continue

            entity = self._entity_repo.get(alias.canonical_entity_id)
            if entity is None:
                continue

            entities_by_id[entity.canonical_entity_id] = entity

        return [
            entities_by_id[canonical_entity_id]
            for canonical_entity_id in sorted(entities_by_id)
        ]


def _decision_from_candidate_set(
    candidate_set: MentionCandidateSet,
    *,
    auto_resolve_threshold: float = 0.96,
) -> ResolutionDecision:
    """Derive a deterministic decision without re-reading candidate repositories."""

    if len(candidate_set.deterministic_hits) == 1:
        return ResolutionDecision(
            selected_entity_id=candidate_set.deterministic_hits[0],
            method=ResolutionMethod.DETERMINISTIC,
            confidence=1.0,
            rationale="single deterministic candidate",
        )

    if (
        not candidate_set.deterministic_hits
        and len(candidate_set.fuzzy_hits) == 1
        and candidate_set.fuzzy_scores.get(candidate_set.fuzzy_hits[0], 0.0)
        >= auto_resolve_threshold
    ):
        entity_id = candidate_set.fuzzy_hits[0]
        return ResolutionDecision(
            selected_entity_id=entity_id,
            method=ResolutionMethod.FUZZY,
            confidence=candidate_set.fuzzy_scores[entity_id],
            rationale="single high-confidence fuzzy candidate",
        )

    if candidate_set.deterministic_hits:
        rationale = "ambiguous deterministic candidates"
    elif candidate_set.fuzzy_hits:
        rationale = "ambiguous or low-confidence fuzzy candidates"
    else:
        rationale = "no deterministic candidates"

    return ResolutionDecision(
        selected_entity_id=None,
        method=ResolutionMethod.UNRESOLVED,
        confidence=None,
        rationale=rationale,
    )


def resolve_mention(
    raw_mention_text: str,
    context: ResolutionContext | dict[str, object] | None = None,
) -> MentionResolutionResult:
    """Resolve one mention through the configured deterministic path."""

    from entity_registry.init import get_default_resolution_repositories

    entity_repo, alias_repo, reference_repo, case_repo = (
        get_default_resolution_repositories()
    )
    return resolve_mention_with_repositories(
        raw_mention_text,
        context,
        entity_repo=entity_repo,
        alias_repo=alias_repo,
        reference_repo=reference_repo,
        case_repo=case_repo,
    )


def resolve_mention_with_repositories(
    raw_mention_text: str,
    context: ResolutionContext | dict[str, object] | None,
    *,
    entity_repo: EntityRepository,
    alias_repo: AliasRepository,
    audit_repo: ResolutionAuditRepository | None = None,
    reference_repo: ReferenceRepository | None = None,
    case_repo: ResolutionCaseRepository | None = None,
    fuzzy_matcher: FuzzyMatcher | None = None,
    ner_extractor: NERExtractor | None = None,
) -> MentionResolutionResult:
    """Resolve one mention using explicit repositories and write audit records."""

    matcher = DeterministicMatcher(entity_repo, alias_repo)
    candidate_set = matcher.collect_candidates_with_fuzzy(
        raw_mention_text,
        context=context,
        fuzzy_matcher=fuzzy_matcher,
        ner_extractor=ner_extractor,
    )
    decision = matcher.resolve_candidate_set(
        candidate_set,
        auto_resolve_threshold=_auto_resolve_threshold(fuzzy_matcher),
    )
    source_context = _source_context_from(context)
    resolved_entity_id = decision.selected_entity_id
    resolution_method = (
        decision.method
        if resolved_entity_id is not None
        else ResolutionMethod.UNRESOLVED
    )
    resolution_confidence = decision.confidence if resolved_entity_id is not None else None
    candidate_entity_ids = _candidate_entity_ids(candidate_set)

    reference = EntityReference(
        reference_id=_new_reference_id(),
        raw_mention_text=raw_mention_text,
        source_context=source_context,
        resolved_entity_id=resolved_entity_id,
        resolution_method=resolution_method,
        resolution_confidence=resolution_confidence,
    )
    case = ResolutionCase(
        case_id=_new_case_id(),
        reference_id=reference.reference_id,
        candidate_entity_ids=candidate_entity_ids,
        selected_entity_id=resolved_entity_id,
        decision_type=(
            DecisionType.AUTO
            if resolved_entity_id is not None or not candidate_entity_ids
            else DecisionType.MANUAL_REVIEW
        ),
        decision_rationale=decision.rationale,
    )
    _save_resolution_audit(
        reference,
        case,
        audit_repo=audit_repo,
        reference_repo=reference_repo,
        case_repo=case_repo,
    )
    return MentionResolutionResult(
        raw_mention_text=raw_mention_text,
        resolved_entity_id=resolved_entity_id,
        resolution_method=resolution_method,
        resolution_confidence=resolution_confidence,
    )


def _save_resolution_audit(
    reference: EntityReference,
    case: ResolutionCase,
    *,
    audit_repo: ResolutionAuditRepository | None,
    reference_repo: ReferenceRepository | None,
    case_repo: ResolutionCaseRepository | None,
) -> None:
    if audit_repo is None:
        if reference_repo is None or case_repo is None:
            raise ResolutionAuditRepositoryRequiredError(
                "resolution audit writes require audit_repo or reference_repo/case_repo"
            )
        audit_repo = _RepositoryResolutionAuditRepository(reference_repo, case_repo)
    audit_repo.save_resolution(reference, case)


def _generate_fuzzy_candidates(
    raw_mention_text: str,
    *,
    context: ResolutionContext | dict[str, object] | None,
    fuzzy_matcher: FuzzyMatcher | None,
    ner_extractor: NERExtractor | None,
    limit: int,
) -> list[FuzzyCandidate]:
    if fuzzy_matcher is None:
        return []

    query_text = _fuzzy_query_text(
        raw_mention_text,
        context=context,
        ner_extractor=ner_extractor,
    )
    return fuzzy_matcher.generate_candidates(query_text, context=context, limit=limit)


def _fuzzy_query_text(
    raw_mention_text: str,
    *,
    context: ResolutionContext | dict[str, object] | None,
    ner_extractor: NERExtractor | None,
) -> str:
    if ner_extractor is None:
        return raw_mention_text

    extracted_mentions = ner_extractor.extract_mentions(
        raw_mention_text,
        context=context,
    )
    if len(extracted_mentions) == 1:
        return extracted_mentions[0].mention_text
    return raw_mention_text


def _candidate_status(
    deterministic_hits: list[str],
    fuzzy_hits: list[str],
    fuzzy_scores: dict[str, float],
    *,
    auto_resolve_threshold: float,
) -> tuple[FinalStatus, bool]:
    if len(deterministic_hits) == 1:
        return FinalStatus.RESOLVED, False
    if deterministic_hits:
        return FinalStatus.MANUAL_REVIEW, True
    if (
        len(fuzzy_hits) == 1
        and fuzzy_scores.get(fuzzy_hits[0], 0.0) >= auto_resolve_threshold
    ):
        return FinalStatus.RESOLVED, False
    if fuzzy_hits:
        return FinalStatus.MANUAL_REVIEW, True
    return FinalStatus.UNRESOLVED, False


def _candidate_entity_ids(candidate_set: MentionCandidateSet) -> list[str]:
    candidate_ids: list[str] = []
    for entity_id in candidate_set.deterministic_hits + candidate_set.fuzzy_hits:
        if entity_id not in candidate_ids:
            candidate_ids.append(entity_id)
    return candidate_ids


def _auto_resolve_threshold(fuzzy_matcher: FuzzyMatcher | None) -> float:
    if fuzzy_matcher is None:
        return 0.96
    threshold = getattr(fuzzy_matcher, "auto_resolve_score", 0.96)
    try:
        threshold_value = float(threshold)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "auto_resolve_score must be finite and between 0.0 and 1.0",
        ) from exc
    if not math.isfinite(threshold_value) or not 0.0 <= threshold_value <= 1.0:
        raise ValueError("auto_resolve_score must be finite and between 0.0 and 1.0")
    return threshold_value


def _source_context_from(
    context: ResolutionContext | dict[str, object] | None,
) -> dict:
    if context is None:
        return {}
    if isinstance(context, ResolutionContext):
        return context.model_dump(mode="json")
    if isinstance(context, dict):
        return dict(context)
    raise TypeError("context must be a ResolutionContext, dict, or None")


__all__ = [
    "DeterministicMatcher",
    "ResolutionAuditRepository",
    "ResolutionAuditRepositoryRequiredError",
    "resolve_mention",
    "resolve_mention_with_repositories",
]
