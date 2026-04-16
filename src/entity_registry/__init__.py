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
    DataPlatformStockBasicReader,
    FileStockBasicSnapshotReader,
    InitializationResult,
    StockBasicRecord,
    StockBasicSnapshotReader,
    detect_cross_listing_groups,
    initialize_from_stock_basic,
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
    "DataPlatformStockBasicReader",
    "DecisionType",
    "EntityAlias",
    "EntityReference",
    "EntityStatus",
    "EntityType",
    "FileStockBasicSnapshotReader",
    "FinalStatus",
    "InitializationResult",
    "MentionCandidateSet",
    "ResolutionCase",
    "ResolutionContext",
    "ResolutionDecision",
    "ResolutionMethod",
    "StockBasicRecord",
    "StockBasicSnapshotReader",
    "detect_cross_listing_groups",
    "generate_aliases_from_stock_basic",
    "generate_stock_entity_id",
    "initialize_from_stock_basic",
    "load_stock_basic_records",
    "lookup_alias",
    "validate_entity_id",
]
