"""Repository protocols and in-memory implementations."""

from __future__ import annotations

from threading import RLock
from typing import Protocol

from entity_registry.core import CanonicalEntity, EntityAlias
from entity_registry.references import EntityReference, ResolutionCase


class EntityRepository(Protocol):
    """Storage contract for canonical entities."""

    def get(self, entity_id: str) -> CanonicalEntity | None: ...

    def save(self, entity: CanonicalEntity) -> None: ...

    def save_if_absent(self, entity: CanonicalEntity) -> bool: ...

    def list_all(self) -> list[CanonicalEntity]: ...

    def exists(self, entity_id: str) -> bool: ...


class AliasRepository(Protocol):
    """Storage contract for entity aliases."""

    def find_by_text(self, alias_text: str) -> list[EntityAlias]: ...

    def find_by_entity(self, entity_id: str) -> list[EntityAlias]: ...

    def save(self, alias: EntityAlias) -> None: ...

    def save_if_absent(self, alias: EntityAlias) -> bool: ...

    def save_batch(self, aliases: list[EntityAlias]) -> None: ...

    def save_batch_if_absent(self, aliases: list[EntityAlias]) -> int: ...


class ReferenceRepository(Protocol):
    """Storage contract for entity references."""

    def save(self, ref: EntityReference) -> None: ...

    def delete(self, reference_id: str) -> None: ...

    def get(self, reference_id: str) -> EntityReference | None: ...

    def find_unresolved(self) -> list[EntityReference]: ...


class ResolutionCaseRepository(Protocol):
    """Storage contract for resolution audit cases."""

    def save(self, case: ResolutionCase) -> None: ...

    def get(self, case_id: str) -> ResolutionCase | None: ...

    def find_by_reference(self, reference_id: str) -> list[ResolutionCase]: ...


class InMemoryEntityRepository:
    """Dictionary-backed entity repository for tests and local workflows."""

    def __init__(self) -> None:
        self._entities: dict[str, CanonicalEntity] = {}
        self._lock = RLock()

    def get(self, entity_id: str) -> CanonicalEntity | None:
        with self._lock:
            return self._entities.get(entity_id)

    def save(self, entity: CanonicalEntity) -> None:
        with self._lock:
            self._entities[entity.canonical_entity_id] = entity

    def save_if_absent(self, entity: CanonicalEntity) -> bool:
        with self._lock:
            if entity.canonical_entity_id in self._entities:
                return False
            self._entities[entity.canonical_entity_id] = entity
            return True

    def list_all(self) -> list[CanonicalEntity]:
        with self._lock:
            return list(self._entities.values())

    def exists(self, entity_id: str) -> bool:
        with self._lock:
            return entity_id in self._entities


class InMemoryAliasRepository:
    """Dictionary-backed alias repository with text and entity indexes."""

    def __init__(self) -> None:
        self._by_text: dict[str, list[EntityAlias]] = {}
        self._by_entity: dict[str, list[EntityAlias]] = {}
        self._semantic_keys: set[tuple[str, str, str]] = set()
        self._lock = RLock()

    def find_by_text(self, alias_text: str) -> list[EntityAlias]:
        with self._lock:
            return list(self._by_text.get(alias_text, []))

    def find_by_entity(self, entity_id: str) -> list[EntityAlias]:
        with self._lock:
            return list(self._by_entity.get(entity_id, []))

    def save(self, alias: EntityAlias) -> None:
        self.save_if_absent(alias)

    def save_if_absent(self, alias: EntityAlias) -> bool:
        with self._lock:
            semantic_key = _alias_semantic_key(alias)
            if semantic_key in self._semantic_keys:
                return False

            self._semantic_keys.add(semantic_key)
            self._save_unchecked(alias)
            return True

    def save_batch(self, aliases: list[EntityAlias]) -> None:
        self.save_batch_if_absent(aliases)

    def save_batch_if_absent(self, aliases: list[EntityAlias]) -> int:
        created = 0
        with self._lock:
            for alias in aliases:
                semantic_key = _alias_semantic_key(alias)
                if semantic_key in self._semantic_keys:
                    continue

                self._semantic_keys.add(semantic_key)
                self._save_unchecked(alias)
                created += 1
        return created

    def _save_unchecked(self, alias: EntityAlias) -> None:
        text_aliases = self._by_text.setdefault(alias.alias_text, [])
        text_aliases.append(alias)

        entity_aliases = self._by_entity.setdefault(alias.canonical_entity_id, [])
        entity_aliases.append(alias)


class InMemoryReferenceRepository:
    """Dictionary-backed reference repository for tests and local workflows."""

    def __init__(self) -> None:
        self._references: dict[str, EntityReference] = {}

    def save(self, ref: EntityReference) -> None:
        self._references[ref.reference_id] = ref

    def delete(self, reference_id: str) -> None:
        self._references.pop(reference_id, None)

    def get(self, reference_id: str) -> EntityReference | None:
        return self._references.get(reference_id)

    def find_unresolved(self) -> list[EntityReference]:
        return [
            ref
            for ref in self._references.values()
            if ref.resolved_entity_id is None
        ]


class InMemoryResolutionCaseRepository:
    """Dictionary-backed resolution case repository for tests and local workflows."""

    def __init__(self) -> None:
        self._cases: dict[str, ResolutionCase] = {}

    def save(self, case: ResolutionCase) -> None:
        self._cases[case.case_id] = case

    def get(self, case_id: str) -> ResolutionCase | None:
        return self._cases.get(case_id)

    def find_by_reference(self, reference_id: str) -> list[ResolutionCase]:
        return [
            case
            for case in self._cases.values()
            if case.reference_id == reference_id
        ]


def _alias_semantic_key(alias: EntityAlias) -> tuple[str, str, str]:
    return (
        alias.canonical_entity_id,
        alias.alias_text,
        alias.alias_type.value,
    )
