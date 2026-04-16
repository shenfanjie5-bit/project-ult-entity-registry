"""Initialization pipeline from stock_basic canonical snapshots."""

from __future__ import annotations

import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Callable, Protocol

from pydantic import BaseModel, field_validator

from entity_registry.aliases import AliasManager, generate_aliases_from_stock_basic
from entity_registry.core import (
    CanonicalEntity,
    EntityStatus,
    EntityType,
    generate_stock_entity_id,
)
from entity_registry.storage import AliasRepository, EntityRepository


DATA_PLATFORM_STOCK_BASIC_REF = "data-platform://canonical/stock_basic"
_DATA_PLATFORM_STOCK_BASIC_REFS = frozenset(
    {
        DATA_PLATFORM_STOCK_BASIC_REF,
        "canonical.stock_basic",
        "stock_basic",
        "latest",
    }
)


class StockBasicRecord(BaseModel):
    """Input row from the data-platform stock_basic canonical table."""

    ts_code: str
    symbol: str
    name: str
    fullname: str | None = None
    enname: str | None = None
    cnspell: str | None = None
    market: str
    exchange: str
    list_status: str
    list_date: str | None = None
    is_hs: str | None = None

    @field_validator("ts_code", "symbol", "name", "market", "exchange", "list_status")
    @classmethod
    def validate_required_text(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("required stock_basic text fields must not be empty")
        return cleaned

    @field_validator("fullname", "enname", "cnspell", "list_date", "is_hs", mode="before")
    @classmethod
    def normalize_optional_text(cls, value: Any) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            value = str(value)

        cleaned = value.strip()
        return cleaned or None


class InitializationResult(BaseModel):
    """Summary counters for one stock_basic initialization run."""

    entities_created: int
    aliases_created: int
    cross_listing_groups: int
    errors: list[str]


class StockBasicSnapshotReader(Protocol):
    """Read stock_basic rows from one snapshot reference."""

    def read(self, snapshot_ref: str) -> list[StockBasicRecord]: ...


class DataPlatformStockBasicReader:
    """Read stock_basic from the data-platform canonical table."""

    def __init__(
        self,
        read_canonical_stock_basic: Callable[..., Any] | None = None,
    ) -> None:
        self._read_canonical_stock_basic = read_canonical_stock_basic

    def read(self, snapshot_ref: str) -> list[StockBasicRecord]:
        _validate_data_platform_stock_basic_ref(snapshot_ref)
        read_canonical_stock_basic = (
            self._read_canonical_stock_basic
            if self._read_canonical_stock_basic is not None
            else _load_data_platform_stock_basic_reader()
        )
        table = read_canonical_stock_basic(active_only=False)
        payload = [
            _normalize_data_platform_stock_basic_row(row)
            for row in _canonical_table_rows(table)
        ]
        return _validate_record_payload(payload, snapshot_ref)


class FixtureStockBasicSnapshotReader:
    """Read stock_basic from JSON/CSV fixtures for tests and local development."""

    def read(self, snapshot_ref: str) -> list[StockBasicRecord]:
        path = Path(snapshot_ref)
        if not path.exists():
            raise FileNotFoundError(snapshot_ref)

        suffix = path.suffix.lower()
        if suffix == ".json":
            payload = _load_json_payload(path)
        elif suffix == ".csv":
            payload = _load_csv_payload(path)
        else:
            raise ValueError("snapshot_ref must point to a JSON or CSV file")

        return _validate_record_payload(payload, snapshot_ref)


def load_stock_basic_records(
    snapshot_ref: str,
    reader: StockBasicSnapshotReader | None = None,
) -> list[StockBasicRecord]:
    """Load stock_basic records through a snapshot reader."""

    return (reader or FixtureStockBasicSnapshotReader()).read(snapshot_ref)


def stock_basic_reader_for_ref(snapshot_ref: str) -> StockBasicSnapshotReader:
    """Return the stock_basic reader for a snapshot reference."""

    if _is_data_platform_stock_basic_ref(snapshot_ref):
        return DataPlatformStockBasicReader()
    return FixtureStockBasicSnapshotReader()


def detect_cross_listing_groups(records: list[StockBasicRecord]) -> dict[str, str]:
    """Detect A+H listings and return a ts_code to cross-listing group mapping."""

    records_by_company: dict[str, list[StockBasicRecord]] = {}
    for record in records:
        key = _cross_listing_company_key(record)
        if key is None:
            continue
        records_by_company.setdefault(key, []).append(record)

    groups: dict[str, str] = {}
    for company_key, company_records in records_by_company.items():
        if not _has_cross_listing_shape(company_records):
            continue

        group_id = _build_cross_listing_group_id(company_key)
        for record in company_records:
            groups[record.ts_code] = group_id

    return groups


def initialize_from_stock_basic(
    snapshot_ref: str,
    entity_repo: EntityRepository,
    alias_repo: AliasRepository,
    stock_basic_reader: StockBasicSnapshotReader | None = None,
) -> InitializationResult:
    """Initialize canonical stock entities and aliases from a stock_basic snapshot."""

    reader = stock_basic_reader or stock_basic_reader_for_ref(snapshot_ref)
    records = reader.read(snapshot_ref)
    cross_listing_groups = detect_cross_listing_groups(records)
    alias_manager = AliasManager(alias_repo)
    entities_created = 0
    aliases_created = 0
    errors: list[str] = []

    for record in records:
        try:
            canonical_entity_id = generate_stock_entity_id(record.ts_code)
            if entity_repo.save_if_absent(
                CanonicalEntity(
                    canonical_entity_id=canonical_entity_id,
                    entity_type=EntityType.STOCK,
                    display_name=record.name,
                    status=_entity_status_from_list_status(record.list_status),
                    anchor_code=record.ts_code,
                    cross_listing_group=cross_listing_groups.get(record.ts_code),
                )
            ):
                entities_created += 1

            aliases_created += alias_manager.add_aliases_batch(
                generate_aliases_from_stock_basic(record, canonical_entity_id)
            )
        except ValueError as exc:
            errors.append(f"{record.ts_code}: {exc}")

    return InitializationResult(
        entities_created=entities_created,
        aliases_created=aliases_created,
        cross_listing_groups=len(set(cross_listing_groups.values())),
        errors=errors,
    )


def _load_data_platform_stock_basic_reader() -> Callable[..., Any]:
    try:
        from data_platform.serving.reader import get_canonical_stock_basic
    except ImportError as exc:
        raise RuntimeError(
            "data-platform serving reader is required for canonical stock_basic reads"
        ) from exc

    return get_canonical_stock_basic


def _canonical_table_rows(table: Any) -> list[dict[str, Any]]:
    if hasattr(table, "to_pylist"):
        rows = table.to_pylist()
    elif isinstance(table, list):
        rows = table
    else:
        raise TypeError("data-platform stock_basic reader must return a table with to_pylist()")

    if not isinstance(rows, list):
        raise TypeError("data-platform stock_basic table to_pylist() must return a list")

    normalized_rows: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"stock_basic canonical row at index {index} is not an object")
        normalized_rows.append(dict(row))

    return normalized_rows


def _normalize_data_platform_stock_basic_row(row: dict[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    if not _has_text(payload.get("exchange")):
        payload["exchange"] = _infer_exchange_from_ts_code(payload.get("ts_code"))
    if not _has_text(payload.get("list_status")):
        payload["list_status"] = _list_status_from_is_active(payload.get("is_active"))
    return payload


def _infer_exchange_from_ts_code(ts_code: Any) -> str:
    if not isinstance(ts_code, str):
        raise ValueError("stock_basic canonical row ts_code must be a string")

    normalized_ts_code = ts_code.strip().upper()
    if normalized_ts_code.endswith(".SH"):
        return "SSE"
    if normalized_ts_code.endswith(".SZ"):
        return "SZSE"
    if normalized_ts_code.endswith(".BJ"):
        return "BSE"
    if normalized_ts_code.endswith(".HK"):
        return "HKEX"

    raise ValueError(f"cannot infer exchange from stock_basic ts_code: {ts_code!r}")


def _list_status_from_is_active(is_active: Any) -> str:
    if isinstance(is_active, bool):
        return "L" if is_active else "D"
    if isinstance(is_active, str):
        normalized = is_active.strip().lower()
        if normalized in {"true", "t", "1", "yes", "y"}:
            return "L"
        if normalized in {"false", "f", "0", "no", "n"}:
            return "D"

    raise ValueError("stock_basic canonical row must include boolean is_active")


def _has_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_data_platform_stock_basic_ref(snapshot_ref: str) -> bool:
    return snapshot_ref.strip() in _DATA_PLATFORM_STOCK_BASIC_REFS


def _validate_data_platform_stock_basic_ref(snapshot_ref: str) -> None:
    if _is_data_platform_stock_basic_ref(snapshot_ref):
        return
    raise ValueError(
        "data-platform stock_basic reader only supports canonical stock_basic refs"
    )


def _load_json_payload(path: Path) -> Any:
    try:
        with path.open(encoding="utf-8") as file_obj:
            return json.load(file_obj)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON stock_basic snapshot: {exc}") from exc


def _load_csv_payload(path: Path) -> list[dict[str, str | None]]:
    with path.open(newline="", encoding="utf-8") as file_obj:
        reader = csv.DictReader(file_obj)
        if reader.fieldnames is None:
            return []
        return list(reader)


def _validate_record_payload(payload: Any, snapshot_ref: str) -> list[StockBasicRecord]:
    if isinstance(payload, dict):
        if isinstance(payload.get("records"), list):
            payload = payload["records"]
        elif isinstance(payload.get("data"), list):
            payload = payload["data"]
        else:
            raise ValueError("stock_basic JSON object must contain a records or data list")

    if not isinstance(payload, list):
        raise ValueError("stock_basic snapshot must contain a list of records")

    records: list[StockBasicRecord] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise ValueError(f"stock_basic record at index {index} is not an object")
        try:
            records.append(StockBasicRecord.model_validate(item))
        except ValueError as exc:
            raise ValueError(
                f"invalid stock_basic record at index {index} in {snapshot_ref}: {exc}"
            ) from exc

    return records


def _entity_status_from_list_status(list_status: str) -> EntityStatus:
    return EntityStatus.ACTIVE if list_status.upper() == "L" else EntityStatus.INACTIVE


def _cross_listing_company_key(record: StockBasicRecord) -> str | None:
    source_name = record.fullname or record.name
    normalized = source_name.lower()
    normalized = re.sub(r"\s+", "", normalized)
    normalized = re.sub(r"[()（）【】\[\]·.,，。-]", "", normalized)
    for suffix in (
        "股份有限公司",
        "有限责任公司",
        "有限公司",
        "集团",
        "companylimited",
        "coltd",
        "limited",
        "incorporated",
    ):
        normalized = normalized.removesuffix(suffix)

    return normalized or None


def _has_cross_listing_shape(records: list[StockBasicRecord]) -> bool:
    has_mainland_listing = any(_is_mainland_listing(record) for record in records)
    has_h_listing = any(_is_h_listing(record) for record in records)
    has_cross_listing_marker = any(_has_cross_listing_marker(record) for record in records)

    return has_mainland_listing and has_h_listing and has_cross_listing_marker


def _is_mainland_listing(record: StockBasicRecord) -> bool:
    exchange = record.exchange.upper()
    ts_code = record.ts_code.upper()
    return exchange in {"SSE", "SZSE", "BSE"} or ts_code.endswith((".SH", ".SZ", ".BJ"))


def _is_h_listing(record: StockBasicRecord) -> bool:
    exchange = record.exchange.upper()
    market = record.market.upper()
    ts_code = record.ts_code.upper()
    return (
        exchange in {"HKEX", "HK", "SEHK"}
        or market in {"HK", "H"}
        or ts_code.endswith(".HK")
    )


def _has_cross_listing_marker(record: StockBasicRecord) -> bool:
    if _is_h_listing(record):
        return True
    if record.is_hs is None:
        return False

    return record.is_hs.upper() in {"H", "S"}


def _build_cross_listing_group_id(company_key: str) -> str:
    digest = hashlib.sha1(company_key.encode("utf-8")).hexdigest()[:12]
    return f"XLG_{digest}"
