"""Regression tests consuming the shared ``audit_eval_fixtures`` package.

Per SUBPROJECT_TESTING_STANDARD.md §10 ``entity-registry`` heavy-uses
``event_cases`` for fuzzy alias / unresolved / A+H boundaries.

This module:

1. Walks every ``event_cases`` case and validates fixture metadata.
2. **Really exercises a runtime function**: drives
   ``entity_registry.aliases.lookup_alias_in_repositories`` (a
   repository-injectable variant that doesn't need configured-default
   repos) using the fixture's ``alias_table_snapshot``; asserts the
   produced ``CanonicalEntity`` for ``case_fuzzy_alias_simple`` matches
   the fixture's expected resolved_entity_id (iron rule #5 + sub-rule).

**Hard-import on purpose** (iron rule #1).

Install path: ``pip install -e ".[dev,shared-fixtures]"``.
"""

from __future__ import annotations

# Hard import — fail collection if shared-fixtures extra not installed.
from audit_eval_fixtures import (  # noqa: F401
    Case,
    CaseRef,
    iter_cases,
    list_packs,
    load_case,
)

# Hard-import the runtime aliases function we exercise.
from entity_registry.aliases import lookup_alias_in_repositories  # noqa: F401


class TestSharedFixturesAreReachable:
    def test_event_cases_pack_present(self) -> None:
        assert "event_cases" in list_packs()


class TestEventCaseMetadataIsContractCompatible:
    REQUIRED_METADATA_KEYS = {
        "fixture_id", "source_module", "contract_version",
        "fixture_kind", "golden_updated_at",
    }

    def test_every_case_metadata_has_required_keys(self) -> None:
        for ref in iter_cases("event_cases"):
            case = load_case(ref.pack_name, ref.case_id)
            missing = self.REQUIRED_METADATA_KEYS - set(case.metadata.keys())
            assert not missing, (
                f"{ref.case_id} missing metadata keys: {missing}"
            )


class TestRuntimeAliasResolutionAgainstFixture:
    """**Real-runtime regression** (iron rule #5).

    For ``case_fuzzy_alias_simple``: the fixture provides an alias
    table containing "宁德" → ENT_STOCK_300750_SZ; the expected
    resolved_entity_id is ENT_STOCK_300750_SZ. We instantiate an
    in-memory entity + alias repository from the fixture's
    ``alias_table_snapshot``, call the runtime
    ``lookup_alias_in_repositories``, and assert the resolved entity's
    canonical_entity_id matches the fixture's expected value.
    """

    def test_case_fuzzy_alias_simple_resolves_to_expected_entity(self) -> None:
        from datetime import UTC, datetime

        from entity_registry.core import (
            AliasType,
            CanonicalEntity,
            EntityAlias,
            EntityStatus,
            EntityType,
        )
        from entity_registry.storage import (
            InMemoryAliasRepository,
            InMemoryEntityRepository,
        )

        case = load_case("event_cases", "case_fuzzy_alias_simple")
        expected_id = case.expected["resolved_entity_id"]

        # Build an in-memory entity + alias state from the fixture.
        entity_repo = InMemoryEntityRepository()
        alias_repo = InMemoryAliasRepository()

        # The fixture's alias_table_snapshot lists multiple aliases
        # for ENT_STOCK_300750_SZ; we need at least one entity record.
        catl = CanonicalEntity(
            canonical_entity_id=expected_id,
            entity_type=EntityType.STOCK,
            display_name="宁德时代",
            status=EntityStatus.ACTIVE,
            anchor_code="300750.SZ",
            cross_listing_group=None,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )
        entity_repo.save(catl)

        alias_type_map = {
            "full_name": AliasType.FULL_NAME,
            "short_name": AliasType.SHORT_NAME,
            "english": AliasType.ENGLISH,
            "former_name": AliasType.FORMER_NAME,
            "code": AliasType.CODE,
            "cnspell": AliasType.CNSPELL,
        }
        for alias_record in case.context["alias_table_snapshot"]:
            alias_repo.save(
                EntityAlias(
                    canonical_entity_id=alias_record["canonical_entity_id"],
                    alias_text=alias_record["alias_text"],
                    alias_type=alias_type_map[alias_record["alias_type"]],
                    is_primary=alias_record.get("is_primary", False),
                    confidence=alias_record["confidence"],
                    source="regression-test",
                    created_at=datetime.now(UTC),
                    updated_at=datetime.now(UTC),
                )
            )

        # Run the runtime alias lookup (positional args: alias_text,
        # alias_repo, entity_repo per aliases.py:78 signature).
        raw_mention = case.input["raw_mention_text"]  # "宁德"
        resolved = lookup_alias_in_repositories(
            raw_mention,
            alias_repo,
            entity_repo,
        )

        # Business expectation: the runtime resolves "宁德" to the
        # expected ENT_* canonical id (sub-rule: keyed to fixture's
        # specific business outcome, not generic invariant).
        assert resolved is not None, (
            f"lookup_alias_in_repositories returned None for {raw_mention!r}"
        )
        assert resolved.canonical_entity_id == expected_id, (
            f"resolved id {resolved.canonical_entity_id!r} != "
            f"fixture expected {expected_id!r}"
        )
        # ENT_* canonical id rule (CLAUDE.md): must start with ENT_.
        assert resolved.canonical_entity_id.startswith("ENT_"), (
            f"canonical_entity_id violates ENT_* rule: {resolved.canonical_entity_id!r}"
        )
