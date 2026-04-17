"""Repository protocols and in-memory implementations."""

from __future__ import annotations

from datetime import UTC, datetime
from threading import RLock
from typing import TYPE_CHECKING, Protocol

from entity_registry.core import CanonicalEntity, EntityAlias
from entity_registry.references import EntityReference, ResolutionCase

if TYPE_CHECKING:
    from entity_registry.review import UnresolvedQueueItem


def _utcnow() -> datetime:
    return datetime.now(UTC)


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

    def list_all(self) -> list[EntityAlias]: ...

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


class ReviewRepository(Protocol):
    """Storage contract for manual review queue items."""

    def save(self, item: UnresolvedQueueItem) -> None: ...

    def get(self, queue_item_id: str) -> UnresolvedQueueItem | None: ...

    def find_by_reference(self, reference_id: str) -> UnresolvedQueueItem | None: ...

    def list_by_status(
        self,
        status: str,
        *,
        limit: int | None = None,
    ) -> list[UnresolvedQueueItem]: ...

    def claim(self, queue_item_id: str, reviewer_id: str) -> UnresolvedQueueItem: ...


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

    def list_all(self) -> list[EntityAlias]:
        with self._lock:
            return [
                alias
                for entity_aliases in self._by_entity.values()
                for alias in entity_aliases
            ]

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
        self._lock = RLock()

    def save(self, ref: EntityReference) -> None:
        with self._lock:
            self._save_unchecked(ref)

    def delete(self, reference_id: str) -> None:
        with self._lock:
            self._references.pop(reference_id, None)

    def get(self, reference_id: str) -> EntityReference | None:
        with self._lock:
            return self._references.get(reference_id)

    def find_unresolved(self) -> list[EntityReference]:
        with self._lock:
            return [
                ref
                for ref in self._references.values()
                if ref.resolved_entity_id is None
            ]

    def _save_unchecked(self, ref: EntityReference) -> None:
        self._references[ref.reference_id] = ref


class InMemoryResolutionAuditReferenceRepository(InMemoryReferenceRepository):
    """In-memory reference repository with native resolution-case audit writes."""

    def __init__(self, case_repo: "InMemoryResolutionCaseRepository") -> None:
        super().__init__()
        self._case_repo = case_repo
        shared_lock = RLock()
        self._lock = shared_lock
        self._case_repo._lock = shared_lock

    def save_resolution(
        self,
        reference: EntityReference,
        case: ResolutionCase,
    ) -> None:
        with self._lock:
            if case.reference_id != reference.reference_id:
                raise ValueError(
                    "resolution case reference_id must match EntityReference",
                )
            self._case_repo._validate_save(case)
            reference_existed = reference.reference_id in self._references
            previous_reference = self._references.get(reference.reference_id)
            case_existed = case.case_id in self._case_repo._cases
            previous_case = self._case_repo._cases.get(case.case_id)

            try:
                self._save_unchecked(reference)
                self._case_repo._save_unchecked(case)
            except Exception:
                if reference_existed and previous_reference is not None:
                    self._references[reference.reference_id] = previous_reference
                else:
                    self._references.pop(reference.reference_id, None)

                if case_existed and previous_case is not None:
                    self._case_repo._cases[case.case_id] = previous_case
                else:
                    self._case_repo._cases.pop(case.case_id, None)
                raise


class InMemoryResolutionCaseRepository:
    """Dictionary-backed resolution case repository for tests and local workflows."""

    def __init__(self) -> None:
        self._cases: dict[str, ResolutionCase] = {}
        self._lock = RLock()

    def save(self, case: ResolutionCase) -> None:
        with self._lock:
            self._validate_save(case)
            self._save_unchecked(case)

    def get(self, case_id: str) -> ResolutionCase | None:
        with self._lock:
            return self._cases.get(case_id)

    def find_by_reference(self, reference_id: str) -> list[ResolutionCase]:
        with self._lock:
            return [
                case
                for case in self._cases.values()
                if case.reference_id == reference_id
            ]

    def _validate_save(self, case: ResolutionCase) -> None:
        return None

    def _save_unchecked(self, case: ResolutionCase) -> None:
        self._cases[case.case_id] = case


class InMemoryReviewRepository:
    """Dictionary-backed review repository with reference-level idempotency."""

    def __init__(self) -> None:
        self._items: dict[str, UnresolvedQueueItem] = {}
        self._by_reference: dict[str, str] = {}
        self._lock = RLock()

    def save(self, item: UnresolvedQueueItem) -> None:
        with self._lock:
            existing_id = self._by_reference.get(item.reference_id)
            if existing_id is not None and existing_id != item.queue_item_id:
                return

            previous = self._items.get(item.queue_item_id)
            if previous is not None and previous.reference_id != item.reference_id:
                self._by_reference.pop(previous.reference_id, None)

            self._items[item.queue_item_id] = item
            self._by_reference[item.reference_id] = item.queue_item_id

    def get(self, queue_item_id: str) -> UnresolvedQueueItem | None:
        with self._lock:
            return self._items.get(queue_item_id)

    def find_by_reference(self, reference_id: str) -> UnresolvedQueueItem | None:
        with self._lock:
            queue_item_id = self._by_reference.get(reference_id)
            if queue_item_id is None:
                return None
            return self._items.get(queue_item_id)

    def list_by_status(
        self,
        status: str,
        *,
        limit: int | None = None,
    ) -> list[UnresolvedQueueItem]:
        if limit is not None and limit < 0:
            raise ValueError("limit must be non-negative")

        status_value = getattr(status, "value", status)
        with self._lock:
            items = [
                item
                for item in self._items.values()
                if item.status == status_value
            ]
            items.sort(key=lambda item: (item.created_at, item.queue_item_id))
            if limit is None:
                return items
            return items[:limit]

    def claim(self, queue_item_id: str, reviewer_id: str) -> UnresolvedQueueItem:
        from entity_registry.review import ReviewNotFoundError, ReviewStateError

        normalized_reviewer_id = reviewer_id.strip()
        if not normalized_reviewer_id:
            raise ValueError("reviewer_id must be a non-empty string")

        with self._lock:
            item = self._items.get(queue_item_id)
            if item is None:
                raise ReviewNotFoundError(
                    f"review queue item not found: {queue_item_id}"
                )

            if item.status == "claimed":
                if item.claimed_by == normalized_reviewer_id:
                    return item
                raise ReviewStateError(
                    f"review queue item already claimed: {queue_item_id}"
                )
            if item.status != "pending":
                raise ReviewStateError(
                    f"review queue item cannot be claimed from status={item.status}"
                )

            updated = item.model_copy(
                update={
                    "status": "claimed",
                    "claimed_by": normalized_reviewer_id,
                    "updated_at": _utcnow(),
                }
            )
            self._items[queue_item_id] = updated
            self._by_reference[updated.reference_id] = queue_item_id
            return updated


def _alias_semantic_key(alias: EntityAlias) -> tuple[str, str, str]:
    return (
        alias.canonical_entity_id,
        alias.alias_text,
        alias.alias_type.value,
    )
