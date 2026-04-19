import os
import subprocess
import sys
import tomllib
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path

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
from entity_registry.init import (
    FileStockBasicSnapshotReader,
    initialize_from_stock_basic_into,
)
from entity_registry.references import (
    EntityReference as RuntimeEntityReference,
    ResolutionCase as RuntimeResolutionCase,
)
from entity_registry.resolution_types import MentionResolutionResult
from entity_registry.storage import (
    InMemoryAliasRepository,
    InMemoryEntityRepository,
    InMemoryResolutionAuditReferenceRepository,
    InMemoryResolutionCaseRepository,
)


NOW = datetime(2026, 4, 15, tzinfo=UTC)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_PATH = PROJECT_ROOT / "tests" / "fixtures" / "stock_basic_sample.json"


@pytest.fixture(autouse=True)
def reset_public_repositories() -> Iterator[None]:
    entity_registry.reset_default_repositories()
    yield
    entity_registry.reset_default_repositories()


def test_installed_contracts_dependency_exports_canonical_id_rule_version(
    tmp_path: Path,
) -> None:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT / "src")
    env["ENTITY_REGISTRY_CONTRACTS_SRC"] = str(
        (PROJECT_ROOT.parent / "contracts" / "src").resolve()
    )
    script = """
import importlib.metadata
import os
import re
import sys
from pathlib import Path

contracts_src = Path(os.environ["ENTITY_REGISTRY_CONTRACTS_SRC"])
for entry in sys.path:
    if entry and Path(entry).resolve() == contracts_src:
        raise AssertionError("sibling contracts/src must not shadow dependency")

import contracts.schemas as contract_schemas
import entity_registry.contracts as registry_contracts

def version_tuple(value):
    match = re.match(r"^(\\d+)\\.(\\d+)\\.(\\d+)", value)
    if match is None:
        raise AssertionError(f"unsupported contracts version: {value}")
    return tuple(int(part) for part in match.groups())

installed_version = importlib.metadata.version("project-ult-contracts")
assert version_tuple(installed_version) >= (0, 1, 0)
assert isinstance(contract_schemas.CANONICAL_ID_RULE_VERSION, str)
assert contract_schemas.CANONICAL_ID_RULE_VERSION
assert (
    registry_contracts.CANONICAL_ID_RULE_VERSION
    == contract_schemas.CANONICAL_ID_RULE_VERSION
)
"""

    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_project_requires_contracts_release_with_rule_version_export() -> None:
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())
    dependencies = pyproject["project"]["dependencies"]

    assert any(
        dependency == "project-ult-contracts>=0.1.0"
        for dependency in dependencies
    )


def test_entity_registry_reexports_contract_entity_schemas() -> None:
    assert registry_contracts.CanonicalEntity is contract_schemas.CanonicalEntity
    assert registry_contracts.EntityAlias is contract_schemas.EntityAlias
    assert registry_contracts.EntityReference is contract_schemas.EntityReference
    assert registry_contracts.ResolutionCase is contract_schemas.ResolutionCase

    assert entity_registry.CanonicalEntity is contract_schemas.CanonicalEntity
    assert entity_registry.EntityAlias is contract_schemas.EntityAlias
    assert entity_registry.EntityReference is contract_schemas.EntityReference
    assert entity_registry.ResolutionCase is contract_schemas.ResolutionCase
    assert entity_registry.ContractCanonicalEntity is contract_schemas.CanonicalEntity
    assert entity_registry.ContractEntityAlias is contract_schemas.EntityAlias
    assert entity_registry.ContractEntityReference is contract_schemas.EntityReference
    assert entity_registry.ContractResolutionCase is contract_schemas.ResolutionCase
    assert not hasattr(entity_registry, "CanonicalEntityProfile")
    assert not hasattr(entity_registry, "MentionResolutionResult")


def test_root_public_api_payloads_validate_against_contract_schemas() -> None:
    entity_repo = InMemoryEntityRepository()
    alias_repo = InMemoryAliasRepository()
    result = initialize_from_stock_basic_into(
        str(FIXTURE_PATH),
        entity_repo,
        alias_repo,
        stock_basic_reader=FileStockBasicSnapshotReader(),
    )
    assert result.errors == []
    entity_registry.configure_default_in_memory_audit_repositories(
        entity_repo,
        alias_repo,
    )

    alias_hit = entity_registry.lookup_alias("贵州茅台")
    assert alias_hit is not None
    contract_schemas.CanonicalEntity.model_validate(
        alias_hit.model_dump(mode="json"),
    )

    resolution_case = entity_registry.resolve_mention(
        "贵州茅台",
        {"source": "contract-boundary-test"},
    )
    contract_schemas.ResolutionCase.model_validate(
        resolution_case.model_dump(mode="json"),
    )

    batch_cases = entity_registry.batch_resolve(["平安银行", "不存在公司"])
    assert len(batch_cases) == 2
    for batch_case in batch_cases:
        contract_schemas.ResolutionCase.model_validate(
            batch_case.model_dump(mode="json"),
        )

    unresolved_case = entity_registry.register_unresolved_reference(
        {
            "reference_id": "ref-contract-boundary",
            "raw_mention_text": "Unknown Corp",
            "source_context": {"source": "contract-boundary-test"},
        }
    )
    contract_schemas.ResolutionCase.model_validate(
        unresolved_case.model_dump(mode="json"),
    )

    profile = entity_registry.get_entity_profile("ENT_STOCK_000001.SZ")
    contract_schemas.CanonicalEntity.model_validate(
        profile["canonical_entity"].model_dump(mode="json"),
    )
    for alias in profile["aliases"]:
        contract_schemas.EntityAlias.model_validate(alias.model_dump(mode="json"))


def test_root_batch_resolve_uses_one_repository_context_for_audit_conversion() -> None:
    entity_repo_a = InMemoryEntityRepository()
    alias_repo_a = InMemoryAliasRepository()
    result = initialize_from_stock_basic_into(
        str(FIXTURE_PATH),
        entity_repo_a,
        alias_repo_a,
        stock_basic_reader=FileStockBasicSnapshotReader(),
    )
    assert result.errors == []

    entity_repo_b = InMemoryEntityRepository()
    alias_repo_b = InMemoryAliasRepository()
    case_repo_a = InMemoryResolutionCaseRepository()
    case_repo_b = InMemoryResolutionCaseRepository()
    reference_repo_b = InMemoryResolutionAuditReferenceRepository(case_repo_b)
    reconfigured = False

    def switch_to_context_b() -> None:
        nonlocal reconfigured
        if reconfigured:
            return
        reconfigured = True
        entity_registry.configure_default_repositories(
            entity_repo_b,
            alias_repo_b,
            reference_repo=reference_repo_b,
            case_repo=case_repo_b,
        )

    reference_repo_a = ReconfiguringAuditReferenceRepository(
        case_repo_a,
        switch_to_context_b,
    )
    entity_registry.configure_default_repositories(
        entity_repo_a,
        alias_repo_a,
        reference_repo=reference_repo_a,
        case_repo=case_repo_a,
    )

    batch_cases = entity_registry.batch_resolve(
        [
            {
                "reference_id": "ref-root-batch-context-race",
                "raw_mention_text": "平安银行",
            }
        ]
    )

    assert reconfigured is True
    default_entity_repo, default_alias_repo = entity_registry.get_default_repositories()
    assert default_entity_repo is entity_repo_b
    assert default_alias_repo is alias_repo_b
    assert case_repo_b.find_by_reference("ref-root-batch-context-race") == []

    persisted_cases = case_repo_a.find_by_reference("ref-root-batch-context-race")
    assert len(persisted_cases) == 1
    assert batch_cases[0].resolution_case_id == persisted_cases[0].case_id
    assert batch_cases[0].resolved_entity is not None
    assert batch_cases[0].resolved_entity.entity_id == "ENT_STOCK_000001.SZ"
    contract_schemas.ResolutionCase.model_validate(
        batch_cases[0].model_dump(mode="json"),
    )


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
    assert contract_case.candidate_entities == []

    dumped = contract_case.model_dump(mode="json")
    assert dumped["candidate_entities"] == []
    assert "ENT_UNRESOLVED_NO_CANDIDATE" not in str(dumped)

    reparsed = contract_schemas.ResolutionCase.model_validate(dumped)
    assert reparsed.candidate_entities == []
    assert reparsed.resolved_entity is None


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


class ReconfiguringAuditReferenceRepository(InMemoryResolutionAuditReferenceRepository):
    def __init__(
        self,
        case_repo: InMemoryResolutionCaseRepository,
        callback: Callable[[], None],
    ) -> None:
        super().__init__(case_repo)
        self._callback = callback

    def save_resolution(
        self,
        reference: RuntimeEntityReference,
        case: RuntimeResolutionCase,
    ) -> None:
        super().save_resolution(reference, case)
        self._callback()
