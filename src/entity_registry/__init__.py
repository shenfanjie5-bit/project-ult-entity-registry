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
    InitializationError,
    InitializationResult,
    RepositoryNotConfiguredError,
    StockBasicRecord,
    StockBasicSnapshotReader,
    configure_default_repositories,
    detect_cross_listing_groups,
    get_default_alias_repository,
    get_default_entity_repository,
    get_default_repositories,
    initialize_from_stock_basic,
    initialize_from_stock_basic_into,
    load_stock_basic_records,
    reset_default_repositories,
)
from entity_registry.references import EntityReference, ResolutionCase
from entity_registry.resolution import DeterministicMatcher
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
    "DeterministicMatcher",
    "EntityAlias",
    "EntityReference",
    "EntityStatus",
    "EntityType",
    "FileStockBasicSnapshotReader",
    "FinalStatus",
    "InitializationError",
    "InitializationResult",
    "MentionCandidateSet",
    "RepositoryNotConfiguredError",
    "ResolutionCase",
    "ResolutionContext",
    "ResolutionDecision",
    "ResolutionMethod",
    "StockBasicRecord",
    "StockBasicSnapshotReader",
    "configure_default_repositories",
    "detect_cross_listing_groups",
    "generate_aliases_from_stock_basic",
    "generate_stock_entity_id",
    "get_default_alias_repository",
    "get_default_entity_repository",
    "get_default_repositories",
    "initialize_from_stock_basic",
    "initialize_from_stock_basic_into",
    "load_stock_basic_records",
    "lookup_alias",
    "reset_default_repositories",
    "validate_entity_id",
]
