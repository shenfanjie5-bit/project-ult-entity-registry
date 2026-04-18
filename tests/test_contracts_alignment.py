from datetime import UTC, datetime

import entity_registry
import entity_registry.contracts as registry_contracts
import contracts.schemas as contract_schemas
import pytest
from contracts.core import __version__ as contracts_package_version

from entity_registry.core import (
    AliasType,
    CanonicalEntity as RuntimeCanonicalEntity,
    DecisionType,
    EntityAlias as RuntimeEntityAlias,
    EntityStatus,
    EntityType,
    ResolutionMethod,
)
from entity_registry.references import (
    EntityReference as RuntimeEntityReference,
    ResolutionCase as RuntimeResolutionCase,
)
from entity_registry.resolution_types import MentionResolutionResult


NOW = datetime(2026, 4, 15, tzinfo=UTC)


def test_entity_registry_reexports_contract_entity_schemas() -> None:
    assert registry_contracts.CanonicalEntity is contract_schemas.CanonicalEntity
    assert registry_contracts.EntityAlias is contract_schemas.EntityAlias
    assert registry_contracts.EntityReference is contract_schemas.EntityReference
    assert registry_contracts.ResolutionCase is contract_schemas.ResolutionCase

    assert entity_registry.ContractCanonicalEntity is contract_schemas.CanonicalEntity
    assert entity_registry.ContractEntityAlias is contract_schemas.EntityAlias
    assert entity_registry.ContractEntityReference is contract_schemas.EntityReference
    assert entity_registry.ContractResolutionCase is contract_schemas.ResolutionCase


def test_canonical_id_rule_version_tracks_entity_id_rule_not_package_release() -> None:
    entity = make_entity()
    alias = make_alias()
    reference = make_reference()
    case = make_case()
    rule_version = registry_contracts.CANONICAL_ID_RULE_VERSION

    assert entity_registry.CANONICAL_ID_RULE_VERSION == rule_version
    assert rule_version != contracts_package_version
    assert entity.canonical_id_rule_version == rule_version
    assert alias.canonical_id_rule_version == rule_version
    assert reference.canonical_id_rule_version == rule_version
    assert case.canonical_id_rule_version == rule_version


def test_internal_canonical_entity_projects_to_contract_schema() -> None:
    entity = make_entity()
    rule_version = registry_contracts.CANONICAL_ID_RULE_VERSION

    contract_entity = registry_contracts.to_contract_canonical_entity(entity)

    assert isinstance(contract_entity, contract_schemas.CanonicalEntity)
    assert contract_entity.model_dump(mode="json") == {
        "canonical_entity_id": "ENT_STOCK_300750.SZ",
        "entity_type": "stock",
        "display_name": "CATL",
        "canonical_id_rule_version": rule_version,
        "created_at": "2026-04-15T00:00:00Z",
        "attributes": {
            "status": "active",
            "anchor_code": "300750.SZ",
            "cross_listing_group": "CATL",
            "updated_at": "2026-04-15T00:00:00Z",
        },
    }


def test_internal_entity_alias_projects_to_contract_schema() -> None:
    alias = make_alias()
    rule_version = registry_contracts.CANONICAL_ID_RULE_VERSION

    contract_alias = registry_contracts.to_contract_entity_alias(alias)

    assert isinstance(contract_alias, contract_schemas.EntityAlias)
    assert contract_alias.canonical_entity_id == "ENT_STOCK_300750.SZ"
    assert contract_alias.alias == "CATL"
    assert contract_alias.alias_type == "short_name"
    assert contract_alias.source_reference == {
        "source": "unit-test",
        "is_primary": True,
    }
    assert contract_alias.canonical_id_rule_version == rule_version


def test_internal_entity_projects_to_contract_entity_reference() -> None:
    entity = make_entity()
    rule_version = registry_contracts.CANONICAL_ID_RULE_VERSION

    contract_reference = registry_contracts.to_contract_entity_reference(entity)

    assert isinstance(contract_reference, contract_schemas.EntityReference)
    assert contract_reference.model_dump(mode="json") == {
        "entity_id": "ENT_STOCK_300750.SZ",
        "entity_type": "stock",
        "canonical_id_rule_version": rule_version,
        "display_name": "CATL",
    }


def test_internal_resolution_case_projects_to_contract_schema() -> None:
    entity = make_entity()
    case = make_case()

    contract_case = registry_contracts.to_contract_resolution_case(
        case,
        input_alias="CATL",
        candidate_entities=[entity],
        decision=contract_schemas.EntityResolutionDecision.MATCHED,
    )

    assert isinstance(contract_case, contract_schemas.ResolutionCase)
    assert contract_case.resolution_case_id == "case-1"
    assert contract_case.input_alias == "CATL"
    assert contract_case.decision is contract_schemas.EntityResolutionDecision.MATCHED
    assert contract_case.confidence == 1.0
    assert contract_case.candidate_entities == [
        registry_contracts.to_contract_entity_reference(entity)
    ]
    assert contract_case.resolved_entity == registry_contracts.to_contract_entity_reference(
        entity
    )
    assert (
        contract_case.canonical_id_rule_version
        == registry_contracts.CANONICAL_ID_RULE_VERSION
    )


@pytest.mark.parametrize(
    "rationale",
    [
        "reasoner declined: both listings remain plausible",
        "reasoner disambiguation failed: timed out",
        "reasoner confidence below threshold 0.70: weak context",
    ],
)
def test_llm_unresolved_multi_candidate_cases_project_as_unresolved(
    rationale: str,
) -> None:
    candidate_entities = [make_hk_entity(), make_entity()]
    case = RuntimeResolutionCase(
        case_id="case-llm-unresolved",
        reference_id="ref-llm-unresolved",
        candidate_entity_ids=[
            "ENT_STOCK_03750.HK",
            "ENT_STOCK_300750.SZ",
        ],
        selected_entity_id=None,
        decision_type=DecisionType.LLM_ASSISTED,
        decision_rationale=rationale,
        created_at=NOW,
    )

    contract_case = registry_contracts.to_contract_resolution_case(
        case,
        input_alias="宁德时代新能源",
        candidate_entities=candidate_entities,
        decision=contract_schemas.EntityResolutionDecision.UNRESOLVED,
    )

    assert contract_case.decision is contract_schemas.EntityResolutionDecision.UNRESOLVED
    assert contract_case.resolved_entity is None
    assert contract_case.confidence == 0.0
    assert contract_case.candidate_entities == [
        registry_contracts.to_contract_entity_reference(candidate)
        for candidate in candidate_entities
    ]


def test_no_candidate_unresolved_case_projects_to_contract_schema() -> None:
    case = RuntimeResolutionCase(
        case_id="case-no-candidate",
        reference_id="ref-no-candidate",
        candidate_entity_ids=[],
        selected_entity_id=None,
        decision_type=DecisionType.AUTO,
        decision_rationale="no candidates found",
        created_at=NOW,
    )

    contract_case = registry_contracts.to_contract_resolution_case(
        case,
        input_alias="不存在公司",
        candidate_entities=[],
        decision=contract_schemas.EntityResolutionDecision.UNRESOLVED,
    )

    assert contract_case.decision is contract_schemas.EntityResolutionDecision.UNRESOLVED
    assert contract_case.resolved_entity is None
    assert contract_case.candidate_entities == [
        contract_schemas.EntityReference(
            entity_id="ENT_UNRESOLVED_NO_CANDIDATE",
            entity_type="unresolved",
            canonical_id_rule_version=registry_contracts.CANONICAL_ID_RULE_VERSION,
        )
    ]


def test_mention_resolution_result_uses_stable_contract_shape() -> None:
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


def test_mention_resolution_result_preserves_raw_mention_whitespace() -> None:
    result = MentionResolutionResult(
        raw_mention_text="  贵州茅台  ",
        resolved_entity_id="ENT_STOCK_600519.SH",
        resolution_method=ResolutionMethod.DETERMINISTIC,
        resolution_confidence=1.0,
    )

    assert result.raw_mention_text == "  贵州茅台  "


def make_entity() -> RuntimeCanonicalEntity:
    return RuntimeCanonicalEntity(
        canonical_entity_id="ENT_STOCK_300750.SZ",
        entity_type=EntityType.STOCK,
        display_name="CATL",
        status=EntityStatus.ACTIVE,
        anchor_code="300750.SZ",
        cross_listing_group="CATL",
        created_at=NOW,
        updated_at=NOW,
    )


def make_hk_entity() -> RuntimeCanonicalEntity:
    return RuntimeCanonicalEntity(
        canonical_entity_id="ENT_STOCK_03750.HK",
        entity_type=EntityType.STOCK,
        display_name="宁德时代",
        status=EntityStatus.ACTIVE,
        anchor_code="03750.HK",
        cross_listing_group="CATL",
        created_at=NOW,
        updated_at=NOW,
    )


def make_alias() -> RuntimeEntityAlias:
    return RuntimeEntityAlias(
        canonical_entity_id="ENT_STOCK_300750.SZ",
        alias_text="CATL",
        alias_type=AliasType.SHORT_NAME,
        confidence=1.0,
        source="unit-test",
        is_primary=True,
        created_at=NOW,
    )


def make_reference() -> RuntimeEntityReference:
    return RuntimeEntityReference(
        reference_id="ref-1",
        raw_mention_text="CATL",
        source_context={"source": "unit-test"},
        resolved_entity_id="ENT_STOCK_300750.SZ",
        resolution_method=ResolutionMethod.DETERMINISTIC,
        resolution_confidence=1.0,
        created_at=NOW,
    )


def make_case() -> RuntimeResolutionCase:
    return RuntimeResolutionCase(
        case_id="case-1",
        reference_id="ref-1",
        candidate_entity_ids=["ENT_STOCK_300750.SZ"],
        selected_entity_id="ENT_STOCK_300750.SZ",
        decision_type=DecisionType.AUTO,
        decision_rationale="unique deterministic match",
        created_at=NOW,
    )
