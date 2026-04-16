"""Alias generation and exact-match alias management."""

from __future__ import annotations

from typing import TYPE_CHECKING

from entity_registry.core import AliasType, EntityAlias
from entity_registry.storage import AliasRepository

if TYPE_CHECKING:
    from entity_registry.init import StockBasicRecord


_STOCK_BASIC_SOURCE = "stock_basic"


def _clean_alias_text(value: str | None) -> str | None:
    if value is None:
        return None

    cleaned = value.strip()
    return cleaned or None


def generate_aliases_from_stock_basic(
    record: StockBasicRecord,
    canonical_entity_id: str,
) -> list[EntityAlias]:
    """Generate stock aliases from the canonical stock_basic fields."""

    alias_specs = [
        (record.name, AliasType.SHORT_NAME, True),
        (record.fullname, AliasType.FULL_NAME, False),
        (record.enname, AliasType.ENGLISH, False),
        (record.cnspell, AliasType.CNSPELL, False),
        (record.symbol, AliasType.CODE, False),
    ]

    aliases: list[EntityAlias] = []
    seen: set[tuple[str, AliasType]] = set()
    for raw_alias_text, alias_type, is_primary in alias_specs:
        alias_text = _clean_alias_text(raw_alias_text)
        if alias_text is None:
            continue

        dedupe_key = (alias_text, alias_type)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        aliases.append(
            EntityAlias(
                canonical_entity_id=canonical_entity_id,
                alias_text=alias_text,
                alias_type=alias_type,
                confidence=1.0,
                source=_STOCK_BASIC_SOURCE,
                is_primary=is_primary,
            )
        )

    return aliases


class AliasManager:
    """Small exact-match facade over an alias repository."""

    def __init__(self, alias_repo: AliasRepository) -> None:
        self._alias_repo = alias_repo

    def add_alias(self, alias: EntityAlias) -> None:
        """Add one alias unless the same entity/text/type mapping already exists."""

        if self._has_semantic_duplicate(alias):
            return

        self._alias_repo.save(alias)

    def add_aliases_batch(self, aliases: list[EntityAlias]) -> int:
        """Add aliases and return the number of newly stored mappings."""

        created = 0
        for alias in aliases:
            if self._has_semantic_duplicate(alias):
                continue
            self._alias_repo.save(alias)
            created += 1

        return created

    def lookup(self, alias_text: str) -> list[EntityAlias]:
        """Return exact text matches for an alias."""

        return self._alias_repo.find_by_text(alias_text)

    def get_entity_aliases(self, canonical_entity_id: str) -> list[EntityAlias]:
        """Return all aliases attached to one canonical entity."""

        return self._alias_repo.find_by_entity(canonical_entity_id)

    def _has_semantic_duplicate(self, alias: EntityAlias) -> bool:
        return any(
            existing.canonical_entity_id == alias.canonical_entity_id
            and existing.alias_text == alias.alias_text
            and existing.alias_type == alias.alias_type
            for existing in self._alias_repo.find_by_text(alias.alias_text)
        )
