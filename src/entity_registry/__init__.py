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
from entity_registry.fuzzy import (
    FuzzyCandidate,
    FuzzyMatcher,
    FuzzyMatcherUnavailable,
    NullFuzzyMatcher,
    SplinkFuzzyMatcher,
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
    CanonicalEntityProfile,
    get_entity_profile,
)
from entity_registry.references import (
    EntityReference,
    ResolutionCase,
    register_unresolved_reference,
)
from entity_registry.resolution import (
    DeterministicMatcher,
    resolve_mention,
)
from entity_registry.resolution_types import (
    BatchResolutionJob,
    MentionCandidateSet,
    MentionResolutionResult,
    ResolutionContext,
    ResolutionDecision,
)
from entity_registry.storage import (
    InMemoryResolutionCaseRepository,
    ResolutionCaseRepository,
)

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "AliasType",
    "AliasManager",
    "BatchResolutionJob",
    "CanonicalEntity",
    "CanonicalEntityProfile",
    "DataPlatformStockBasicReader",
    "DecisionType",
    "DeterministicMatcher",
    "EntityAlias",
    "EntityReference",
    "EntityStatus",
    "EntityType",
    "ExtractedMention",
    "FileStockBasicSnapshotReader",
    "FinalStatus",
    "FuzzyCandidate",
    "FuzzyMatcher",
    "FuzzyMatcherUnavailable",
    "HanLPNERExtractor",
    "InMemoryResolutionCaseRepository",
    "InitializationError",
    "InitializationResult",
    "MentionCandidateSet",
    "MentionResolutionResult",
    "NERExtractor",
    "NullFuzzyMatcher",
    "NullNERExtractor",
    "RepositoryNotConfiguredError",
    "ResolutionCase",
    "ResolutionCaseRepository",
    "ResolutionContext",
    "ResolutionDecision",
    "ResolutionMethod",
    "StockBasicRecord",
    "StockBasicSnapshotReader",
    "SplinkFuzzyMatcher",
    "configure_default_in_memory_audit_repositories",
    "configure_default_repositories",
    "detect_cross_listing_groups",
    "generate_aliases_from_stock_basic",
    "generate_stock_entity_id",
    "get_default_alias_repository",
    "get_default_entity_repository",
    "get_default_repositories",
    "get_entity_profile",
    "initialize_from_stock_basic",
    "initialize_from_stock_basic_into",
    "load_stock_basic_records",
    "lookup_alias",
    "register_unresolved_reference",
    "reset_default_repositories",
    "resolve_mention",
    "validate_entity_id",
]
