import json
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path

import pytest

import entity_registry.init as init_module
from entity_registry.core import AliasType, EntityStatus
from entity_registry.init import (
    DATA_PLATFORM_STOCK_BASIC_REF,
    DataPlatformStockBasicReader,
    InitializationResult,
    StockBasicRecord,
    detect_cross_listing_groups,
    initialize_from_stock_basic,
    load_stock_basic_records,
)
from entity_registry.storage import InMemoryAliasRepository, InMemoryEntityRepository


FIXTURE_PATH = Path("tests/fixtures/stock_basic_sample.json")


def test_stock_basic_record_normalizes_optional_empty_strings() -> None:
    record = StockBasicRecord(
        ts_code="300750.SZ",
        symbol="300750",
        name="宁德时代",
        fullname="",
        enname=" ",
        cnspell="NDSD",
        market="创业板",
        exchange="SZSE",
        list_status="L",
        list_date="",
        is_hs="",
    )

    assert record.fullname is None
    assert record.enname is None
    assert record.list_date is None
    assert record.is_hs is None


def test_initialization_result_builds() -> None:
    result = InitializationResult(
        entities_created=1,
        aliases_created=2,
        cross_listing_groups=0,
        errors=[],
    )

    assert result.entities_created == 1
    assert result.errors == []


def test_load_stock_basic_records_from_json_fixture() -> None:
    records = load_stock_basic_records(str(FIXTURE_PATH))

    assert len(records) == 24
    assert records[0].ts_code == "000001.SZ"
    assert records[0].name == "平安银行"


def test_load_stock_basic_records_supports_records_object(tmp_path: Path) -> None:
    snapshot = tmp_path / "stock_basic.json"
    snapshot.write_text(
        json.dumps({"records": [make_minimal_record_payload()]}),
        encoding="utf-8",
    )

    records = load_stock_basic_records(str(snapshot))

    assert len(records) == 1
    assert records[0].ts_code == "300750.SZ"


def test_load_stock_basic_records_supports_data_object(tmp_path: Path) -> None:
    snapshot = tmp_path / "stock_basic.json"
    snapshot.write_text(
        json.dumps({"data": [make_minimal_record_payload()]}),
        encoding="utf-8",
    )

    records = load_stock_basic_records(str(snapshot))

    assert len(records) == 1


def test_load_stock_basic_records_from_csv(tmp_path: Path) -> None:
    snapshot = tmp_path / "stock_basic.csv"
    snapshot.write_text(
        "\n".join(
            [
                "ts_code,symbol,name,fullname,enname,cnspell,market,exchange,list_status,list_date,is_hs",
                "300750.SZ,300750,宁德时代,宁德时代新能源科技股份有限公司,CATL,NDSD,创业板,SZSE,L,20180611,H",
            ]
        ),
        encoding="utf-8",
    )

    records = load_stock_basic_records(str(snapshot))

    assert len(records) == 1
    assert records[0].symbol == "300750"


def test_load_stock_basic_records_missing_path_raises_file_not_found() -> None:
    with pytest.raises(FileNotFoundError):
        load_stock_basic_records("tests/fixtures/missing_stock_basic.json")


def test_load_stock_basic_records_invalid_json_raises_value_error(tmp_path: Path) -> None:
    snapshot = tmp_path / "bad.json"
    snapshot.write_text("{not-json", encoding="utf-8")

    with pytest.raises(ValueError, match="invalid JSON"):
        load_stock_basic_records(str(snapshot))


def test_load_stock_basic_records_rejects_wrong_json_shape(tmp_path: Path) -> None:
    snapshot = tmp_path / "bad.json"
    snapshot.write_text(json.dumps({"unexpected": []}), encoding="utf-8")

    with pytest.raises(ValueError, match="records or data"):
        load_stock_basic_records(str(snapshot))


def test_load_stock_basic_records_rejects_invalid_record(tmp_path: Path) -> None:
    snapshot = tmp_path / "bad.json"
    payload = make_minimal_record_payload()
    payload["name"] = ""
    snapshot.write_text(json.dumps([payload]), encoding="utf-8")

    with pytest.raises(ValueError, match="invalid stock_basic record"):
        load_stock_basic_records(str(snapshot))


def test_load_stock_basic_records_allows_missing_optional_fields(tmp_path: Path) -> None:
    snapshot = tmp_path / "stock_basic.json"
    payload = make_minimal_record_payload()
    del payload["enname"]
    del payload["cnspell"]
    del payload["is_hs"]
    snapshot.write_text(json.dumps([payload]), encoding="utf-8")

    records = load_stock_basic_records(str(snapshot))

    assert records[0].enname is None
    assert records[0].cnspell is None
    assert records[0].is_hs is None


def test_load_stock_basic_records_rejects_unsupported_suffix(tmp_path: Path) -> None:
    snapshot = tmp_path / "stock_basic.txt"
    snapshot.write_text("[]", encoding="utf-8")

    with pytest.raises(ValueError, match="JSON or CSV"):
        load_stock_basic_records(str(snapshot))


def test_data_platform_stock_basic_reader_maps_canonical_table_rows() -> None:
    calls: list[bool] = []

    def read_canonical_stock_basic(*, active_only: bool = True) -> FakeCanonicalTable:
        calls.append(active_only)
        return FakeCanonicalTable(
            [
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "平安银行",
                    "market": "主板",
                    "list_date": date(1991, 4, 3),
                    "is_active": True,
                    "source_run_id": "run-001",
                },
                {
                    "ts_code": "000002.SZ",
                    "symbol": "000002",
                    "name": "万科A",
                    "market": "主板",
                    "list_date": date(1991, 1, 29),
                    "is_active": False,
                    "source_run_id": "run-001",
                },
            ]
        )

    reader = DataPlatformStockBasicReader(read_canonical_stock_basic)
    records = reader.read(DATA_PLATFORM_STOCK_BASIC_REF)

    assert calls == [False]
    assert [record.exchange for record in records] == ["SZSE", "SZSE"]
    assert [record.list_status for record in records] == ["L", "D"]
    assert records[0].list_date == "1991-04-03"


def test_detect_cross_listing_groups_identifies_fixture_pairs() -> None:
    groups = detect_cross_listing_groups(load_stock_basic_records(str(FIXTURE_PATH)))

    assert groups["300750.SZ"] == groups["03750.HK"]
    assert groups["601318.SH"] == groups["02318.HK"]
    assert len(set(groups.values())) == 2
    assert set(groups) == {"300750.SZ", "03750.HK", "601318.SH", "02318.HK"}


def test_detect_cross_listing_groups_requires_a_and_h_shape() -> None:
    records = [
        StockBasicRecord.model_validate(make_minimal_record_payload()),
        StockBasicRecord.model_validate(
            make_minimal_record_payload(ts_code="300751.SZ", symbol="300751")
        ),
    ]

    assert detect_cross_listing_groups(records) == {}


def test_initialize_from_stock_basic_creates_entities_for_all_fixture_records() -> None:
    entity_repo = InMemoryEntityRepository()
    alias_repo = InMemoryAliasRepository()

    result = initialize_from_stock_basic(str(FIXTURE_PATH), entity_repo, alias_repo)

    assert result.entities_created == 24
    assert len(entity_repo.list_all()) == 24
    assert result.errors == []


def test_initialize_from_stock_basic_uses_data_platform_reader_interface(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entity_repo = InMemoryEntityRepository()
    alias_repo = InMemoryAliasRepository()
    calls: list[bool] = []

    def read_canonical_stock_basic(*, active_only: bool = True) -> FakeCanonicalTable:
        calls.append(active_only)
        return FakeCanonicalTable(
            [
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "平安银行",
                    "market": "主板",
                    "list_date": date(1991, 4, 3),
                    "is_active": True,
                    "source_run_id": "run-001",
                },
                {
                    "ts_code": "000002.SZ",
                    "symbol": "000002",
                    "name": "万科A",
                    "market": "主板",
                    "list_date": date(1991, 1, 29),
                    "is_active": False,
                    "source_run_id": "run-001",
                },
            ]
        )

    monkeypatch.setattr(
        init_module,
        "_load_data_platform_stock_basic_reader",
        lambda: read_canonical_stock_basic,
    )

    result = initialize_from_stock_basic(
        DATA_PLATFORM_STOCK_BASIC_REF,
        entity_repo,
        alias_repo,
    )

    assert calls == [False]
    assert result == InitializationResult(
        entities_created=2,
        aliases_created=4,
        cross_listing_groups=0,
        errors=[],
    )
    assert entity_repo.get("ENT_STOCK_000001.SZ").status is EntityStatus.ACTIVE
    assert entity_repo.get("ENT_STOCK_000002.SZ").status is EntityStatus.INACTIVE
    assert alias_repo.find_by_text("000001")[0].canonical_entity_id == "ENT_STOCK_000001.SZ"


def test_initialize_from_stock_basic_creates_required_aliases() -> None:
    entity_repo = InMemoryEntityRepository()
    alias_repo = InMemoryAliasRepository()

    result = initialize_from_stock_basic(str(FIXTURE_PATH), entity_repo, alias_repo)

    assert result.aliases_created >= 48
    for entity in entity_repo.list_all():
        aliases = alias_repo.find_by_entity(entity.canonical_entity_id)
        alias_types = {alias.alias_type for alias in aliases}
        assert AliasType.SHORT_NAME in alias_types
        assert AliasType.CODE in alias_types


def test_initialize_from_stock_basic_sets_active_and_inactive_statuses() -> None:
    entity_repo = InMemoryEntityRepository()
    alias_repo = InMemoryAliasRepository()

    initialize_from_stock_basic(str(FIXTURE_PATH), entity_repo, alias_repo)

    active_entity = entity_repo.get("ENT_STOCK_300750.SZ")
    inactive_entity = entity_repo.get("ENT_STOCK_000003.SZ")

    assert active_entity is not None
    assert inactive_entity is not None
    assert active_entity.status is EntityStatus.ACTIVE
    assert inactive_entity.status is EntityStatus.INACTIVE


def test_initialize_from_stock_basic_is_idempotent() -> None:
    entity_repo = InMemoryEntityRepository()
    alias_repo = InMemoryAliasRepository()

    first = initialize_from_stock_basic(str(FIXTURE_PATH), entity_repo, alias_repo)
    second = initialize_from_stock_basic(str(FIXTURE_PATH), entity_repo, alias_repo)

    assert first.entities_created == 24
    assert second.entities_created == 0
    assert second.aliases_created == 0
    assert len(entity_repo.list_all()) == 24


def test_initialize_from_stock_basic_uses_atomic_entity_insert() -> None:
    entity_repo = NoExistsEntityRepository()
    alias_repo = InMemoryAliasRepository()

    result = initialize_from_stock_basic(str(FIXTURE_PATH), entity_repo, alias_repo)

    assert result.entities_created == 24
    assert len(entity_repo.list_all()) == 24


def test_initialize_from_stock_basic_is_idempotent_under_concurrent_runs() -> None:
    entity_repo = InMemoryEntityRepository()
    alias_repo = InMemoryAliasRepository()

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(
            executor.map(
                lambda _: initialize_from_stock_basic(
                    str(FIXTURE_PATH),
                    entity_repo,
                    alias_repo,
                ),
                range(4),
            )
        )

    assert sum(result.entities_created for result in results) == 24
    assert len(entity_repo.list_all()) == 24

    stored_aliases = [
        alias
        for entity in entity_repo.list_all()
        for alias in alias_repo.find_by_entity(entity.canonical_entity_id)
    ]
    semantic_keys = {
        (alias.canonical_entity_id, alias.alias_text, alias.alias_type)
        for alias in stored_aliases
    }
    assert len(stored_aliases) == len(semantic_keys)
    assert sum(result.aliases_created for result in results) == len(stored_aliases)


def test_initialize_from_stock_basic_keeps_a_and_h_independent_but_linked() -> None:
    entity_repo = InMemoryEntityRepository()
    alias_repo = InMemoryAliasRepository()

    result = initialize_from_stock_basic(str(FIXTURE_PATH), entity_repo, alias_repo)

    a_share = entity_repo.get("ENT_STOCK_300750.SZ")
    h_share = entity_repo.get("ENT_STOCK_03750.HK")
    assert a_share is not None
    assert h_share is not None
    assert a_share.canonical_entity_id != h_share.canonical_entity_id
    assert a_share.cross_listing_group == h_share.cross_listing_group
    assert a_share.cross_listing_group is not None
    assert result.cross_listing_groups == 2


def test_initialize_from_stock_basic_allows_duplicate_short_name_for_a_and_h() -> None:
    entity_repo = InMemoryEntityRepository()
    alias_repo = InMemoryAliasRepository()

    initialize_from_stock_basic(str(FIXTURE_PATH), entity_repo, alias_repo)

    aliases = alias_repo.find_by_text("宁德时代")
    assert {alias.canonical_entity_id for alias in aliases} == {
        "ENT_STOCK_300750.SZ",
        "ENT_STOCK_03750.HK",
    }


def test_initialize_from_stock_basic_empty_snapshot_returns_zero_counts(tmp_path: Path) -> None:
    snapshot = tmp_path / "empty.json"
    snapshot.write_text("[]", encoding="utf-8")

    result = initialize_from_stock_basic(
        str(snapshot),
        InMemoryEntityRepository(),
        InMemoryAliasRepository(),
    )

    assert result == InitializationResult(
        entities_created=0,
        aliases_created=0,
        cross_listing_groups=0,
        errors=[],
    )


def test_initialize_from_stock_basic_reports_invalid_entity_id(tmp_path: Path) -> None:
    snapshot = tmp_path / "bad-id.json"
    payload = make_minimal_record_payload(ts_code="300750 SZ")
    snapshot.write_text(json.dumps([payload]), encoding="utf-8")

    result = initialize_from_stock_basic(
        str(snapshot),
        InMemoryEntityRepository(),
        InMemoryAliasRepository(),
    )

    assert result.entities_created == 0
    assert result.aliases_created == 0
    assert result.errors


def make_minimal_record_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "ts_code": "300750.SZ",
        "symbol": "300750",
        "name": "宁德时代",
        "fullname": "宁德时代新能源科技股份有限公司",
        "enname": "Contemporary Amperex Technology Co., Limited",
        "cnspell": "NDSD",
        "market": "创业板",
        "exchange": "SZSE",
        "list_status": "L",
        "list_date": "20180611",
        "is_hs": "H",
    }
    payload.update(overrides)
    return payload


class NoExistsEntityRepository(InMemoryEntityRepository):
    def exists(self, entity_id: str) -> bool:
        raise AssertionError(f"exists() should not be used for {entity_id}")


class FakeCanonicalTable:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def to_pylist(self) -> list[dict[str, object]]:
        return self._rows
