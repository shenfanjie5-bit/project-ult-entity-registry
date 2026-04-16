"""Core public exports for the entity-registry package."""

from entity_registry.core import (
    AliasType,
    CanonicalEntity,
    DecisionType,
    EntityAlias,
    EntityStatus,
    EntityType,
    FinalStatus,
    ResolutionMethod,
    generate_stock_entity_id,
    validate_entity_id,
)
from entity_registry.references import EntityReference, ResolutionCase
from entity_registry.resolution_types import (
    BatchResolutionJob,
    MentionCandidateSet,
    ResolutionContext,
    ResolutionDecision,
)

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "AliasType",
    "BatchResolutionJob",
    "CanonicalEntity",
    "DecisionType",
    "EntityAlias",
    "EntityReference",
    "EntityStatus",
    "EntityType",
    "FinalStatus",
    "MentionCandidateSet",
    "ResolutionCase",
    "ResolutionContext",
    "ResolutionDecision",
    "ResolutionMethod",
    "generate_stock_entity_id",
    "validate_entity_id",
]
