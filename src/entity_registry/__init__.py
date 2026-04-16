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
from entity_registry.aliases import (
    AliasManager,
    generate_aliases_from_stock_basic,
    lookup_alias,
)
from entity_registry.init import (
    InitializationError,
    InitializationResult,
    StockBasicRecord,
    detect_cross_listing_groups,
    get_default_alias_repository,
    get_default_entity_repository,
    initialize_from_stock_basic,
    initialize_from_stock_basic_into,
    load_stock_basic_records,
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
    "AliasManager",
    "BatchResolutionJob",
    "CanonicalEntity",
    "DecisionType",
    "EntityAlias",
    "EntityReference",
    "EntityStatus",
    "EntityType",
    "FinalStatus",
    "InitializationError",
    "InitializationResult",
    "MentionCandidateSet",
    "ResolutionCase",
    "ResolutionContext",
    "ResolutionDecision",
    "ResolutionMethod",
    "StockBasicRecord",
    "detect_cross_listing_groups",
    "generate_aliases_from_stock_basic",
    "generate_stock_entity_id",
    "get_default_alias_repository",
    "get_default_entity_repository",
    "initialize_from_stock_basic",
    "initialize_from_stock_basic_into",
    "load_stock_basic_records",
    "lookup_alias",
    "validate_entity_id",
]
