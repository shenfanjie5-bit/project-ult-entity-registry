"""Repository protocols and in-memory implementations."""

from __future__ import annotations

from typing import Protocol

from entity_registry.core import CanonicalEntity, EntityAlias
from entity_registry.references import EntityReference


class EntityRepository(Protocol):
    """Storage contract for canonical entities."""

    def get(self, entity_id: str) -> CanonicalEntity | None: ...

    def save(self, entity: CanonicalEntity) -> None: ...

    def list_all(self) -> list[CanonicalEntity]: ...

    def exists(self, entity_id: str) -> bool: ...


class AliasRepository(Protocol):
    """Storage contract for entity aliases."""

    def find_by_text(self, alias_text: str) -> list[EntityAlias]: ...

    def find_by_entity(self, entity_id: str) -> list[EntityAlias]: ...

    def save(self, alias: EntityAlias) -> None: ...

    def save_batch(self, aliases: list[EntityAlias]) -> None: ...


class ReferenceRepository(Protocol):
    """Storage contract for entity references."""

    def save(self, ref: EntityReference) -> None: ...

    def get(self, reference_id: str) -> EntityReference | None: ...

    def find_unresolved(self) -> list[EntityReference]: ...


class InMemoryEntityRepository:
    """Dictionary-backed entity repository for tests and local workflows."""

    def __init__(self) -> None:
        self._entities: dict[str, CanonicalEntity] = {}

    def get(self, entity_id: str) -> CanonicalEntity | None:
        return self._entities.get(entity_id)

    def save(self, entity: CanonicalEntity) -> None:
        self._entities[entity.canonical_entity_id] = entity

    def list_all(self) -> list[CanonicalEntity]:
        return list(self._entities.values())

    def exists(self, entity_id: str) -> bool:
        return entity_id in self._entities


class InMemoryAliasRepository:
    """Dictionary-backed alias repository with text and entity indexes."""

    def __init__(self) -> None:
        self._by_text: dict[str, list[EntityAlias]] = {}
        self._by_entity: dict[str, list[EntityAlias]] = {}

    def find_by_text(self, alias_text: str) -> list[EntityAlias]:
        return list(self._by_text.get(alias_text, []))

    def find_by_entity(self, entity_id: str) -> list[EntityAlias]:
        return list(self._by_entity.get(entity_id, []))

    def save(self, alias: EntityAlias) -> None:
        text_aliases = self._by_text.setdefault(alias.alias_text, [])
        if alias not in text_aliases:
            text_aliases.append(alias)

        entity_aliases = self._by_entity.setdefault(alias.canonical_entity_id, [])
        if alias not in entity_aliases:
            entity_aliases.append(alias)

    def save_batch(self, aliases: list[EntityAlias]) -> None:
        for alias in aliases:
            self.save(alias)


class InMemoryReferenceRepository:
    """Dictionary-backed reference repository for tests and local workflows."""

    def __init__(self) -> None:
        self._references: dict[str, EntityReference] = {}

    def save(self, ref: EntityReference) -> None:
        self._references[ref.reference_id] = ref

    def get(self, reference_id: str) -> EntityReference | None:
        return self._references.get(reference_id)

    def find_unresolved(self) -> list[EntityReference]:
        return [
            ref
            for ref in self._references.values()
            if ref.resolved_entity_id is None
        ]
