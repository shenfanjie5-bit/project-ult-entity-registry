"""Deterministic mention matching."""

from __future__ import annotations

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
from entity_registry.references import (
    EntityReference,
    ResolutionCase,
    _new_case_id,
    _new_reference_id,
    record_resolution_case,
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

    def collect_candidates(self, raw_mention_text: str) -> MentionCandidateSet:
        """Collect the first non-empty deterministic candidate set."""

        candidates = self.exact_match(raw_mention_text)
        if not candidates:
            candidates = self.code_match(raw_mention_text)
        if not candidates:
            candidates = self.rule_match(raw_mention_text)

        deterministic_hits = [
            entity.canonical_entity_id
            for entity in candidates
        ]
        if len(deterministic_hits) == 1:
            final_status = FinalStatus.RESOLVED
            llm_required = False
        elif deterministic_hits:
            final_status = FinalStatus.MANUAL_REVIEW
            llm_required = True
        else:
            final_status = FinalStatus.UNRESOLVED
            llm_required = False

        return MentionCandidateSet(
            raw_mention_text=raw_mention_text,
            deterministic_hits=deterministic_hits,
            fuzzy_hits=[],
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
    ) -> ResolutionDecision:
        """Return a deterministic decision from a pre-collected candidate set."""

        return _decision_from_candidate_set(candidate_set)

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
) -> ResolutionDecision:
    """Derive a deterministic decision without re-reading candidate repositories."""

    if candidate_set.final_status is FinalStatus.RESOLVED:
        return ResolutionDecision(
            selected_entity_id=candidate_set.deterministic_hits[0],
            method=ResolutionMethod.DETERMINISTIC,
            confidence=1.0,
            rationale="single deterministic candidate",
        )

    if candidate_set.deterministic_hits:
        rationale = "ambiguous deterministic candidates"
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
    reference_repo: ReferenceRepository,
    case_repo: ResolutionCaseRepository,
) -> MentionResolutionResult:
    """Resolve one mention using explicit repositories and write audit records."""

    matcher = DeterministicMatcher(entity_repo, alias_repo)
    candidate_set = matcher.collect_candidates(raw_mention_text)
    decision = matcher.resolve_candidate_set(candidate_set)
    source_context = _source_context_from(context)

    if decision.selected_entity_id is not None:
        reference = EntityReference(
            reference_id=_new_reference_id(),
            raw_mention_text=raw_mention_text,
            source_context=source_context,
            resolved_entity_id=decision.selected_entity_id,
            resolution_method=decision.method,
            resolution_confidence=decision.confidence,
        )
        case = ResolutionCase(
            case_id=_new_case_id(),
            reference_id=reference.reference_id,
            candidate_entity_ids=candidate_set.deterministic_hits,
            selected_entity_id=decision.selected_entity_id,
            decision_type=DecisionType.AUTO,
            decision_rationale=decision.rationale,
        )
        _save_resolution_audit(
            reference,
            case,
            reference_repo=reference_repo,
            case_repo=case_repo,
        )
        return MentionResolutionResult(
            raw_mention_text=raw_mention_text,
            resolved_entity_id=decision.selected_entity_id,
            resolution_method=decision.method,
            resolution_confidence=decision.confidence,
        )

    reference = EntityReference(
        reference_id=_new_reference_id(),
        raw_mention_text=raw_mention_text,
        source_context=source_context,
        resolved_entity_id=None,
        resolution_method=ResolutionMethod.UNRESOLVED,
        resolution_confidence=None,
    )
    case = ResolutionCase(
        case_id=_new_case_id(),
        reference_id=reference.reference_id,
        candidate_entity_ids=candidate_set.deterministic_hits,
        selected_entity_id=None,
        decision_type=(
            DecisionType.MANUAL_REVIEW
            if candidate_set.deterministic_hits
            else DecisionType.AUTO
        ),
        decision_rationale=decision.rationale,
    )
    _save_resolution_audit(
        reference,
        case,
        reference_repo=reference_repo,
        case_repo=case_repo,
    )
    return MentionResolutionResult(
        raw_mention_text=raw_mention_text,
        resolved_entity_id=None,
        resolution_method=ResolutionMethod.UNRESOLVED,
        resolution_confidence=None,
    )


def _save_resolution_audit(
    reference: EntityReference,
    case: ResolutionCase,
    *,
    reference_repo: ReferenceRepository,
    case_repo: ResolutionCaseRepository,
) -> None:
    reference_repo.save(reference)
    try:
        record_resolution_case(case, case_repo)
    except Exception:
        reference_repo.delete(reference.reference_id)
        raise


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
    "resolve_mention",
    "resolve_mention_with_repositories",
]
