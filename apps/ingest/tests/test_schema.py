"""schema/tokyo/*.json（新テーブルの定義。11b の Terraform が読む）の検査（AC 12）"""

import json

import pytest
from conftest import REPO_ROOT

OLD = REPO_ROOT / "schema"
NEW = REPO_ROOT / "schema" / "tokyo"
TABLES = ("spot_price", "future_price", "option_price")
ADDED = {"ingest_run_id", "parser_version", "source_object", "page_updated_at", "price_time_raw"}


def load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def cols(schema):
    return {c["name"]: (c["type"], c.get("mode", "NULLABLE")) for c in schema}


@pytest.mark.parametrize("table", TABLES)
def test_existing_columns_unchanged(table):
    old = cols(load(OLD / f"{table}.json"))
    new = cols(load(NEW / f"{table}.json"))
    for name, spec in old.items():
        assert new[name] == spec, name


@pytest.mark.parametrize("table", TABLES)
def test_added_columns_are_nullable(table):
    old = cols(load(OLD / f"{table}.json"))
    new = cols(load(NEW / f"{table}.json"))
    added = set(new) - set(old)
    assert added == ADDED
    for name in added:
        assert new[name][1] == "NULLABLE", name


@pytest.mark.parametrize("table", TABLES + ("ingest_runs",))
def test_every_column_has_description_and_mode(table):
    for c in load(NEW / f"{table}.json"):
        assert c["description"], c["name"]
        assert c["mode"] in ("NULLABLE", "REQUIRED"), c["name"]


def test_tables_settings():
    settings = load(NEW / "tables.json")
    assert settings["dataset"]["dataset_id"] == "optionchan_tokyo"
    assert settings["dataset"]["location"] == "asia-northeast1"
    assert set(settings["tables"]) == set(TABLES) | {"ingest_runs"}
    for table, spec in settings["tables"].items():
        names = cols(load(NEW / f"{table}.json"))
        field = spec["time_partitioning"]["field"]
        assert names[field] == ("TIMESTAMP", "REQUIRED"), table
        for c in spec["clustering"]:
            assert c in names, (table, c)
    assert settings["tables"]["option_price"]["require_partition_filter"] is True


# 取引カレンダー（#63）。列・型・モードはオーナー承認済みの契約（simulator の契約テストもこのファイルを照合する）。
# 変えるときは読み手と合意してから（列の追加は NULLABLE で末尾に。改名・削除・型の変更はしない）
TRADING_CALENDAR = [
    ("version_at", "TIMESTAMP", "REQUIRED"),
    ("calendar_date", "DATE", "REQUIRED"),
    ("is_trading_day", "BOOLEAN", "REQUIRED"),
    ("status", "STRING", "REQUIRED"),
    ("day_type", "STRING", "REQUIRED"),
    ("name", "STRING", "REQUIRED"),
    ("note", "STRING", "NULLABLE"),
    ("source", "STRING", "REQUIRED"),
    ("covered_until", "DATE", "REQUIRED"),
    ("listed_until", "DATE", "REQUIRED"),
    ("content_hash", "STRING", "REQUIRED"),
    ("accepted", "STRING", "REQUIRED"),
]


def test_trading_calendar_contract():
    schema = load(NEW / "trading_calendar.json")
    assert [(c["name"], c["type"], c["mode"]) for c in schema] == TRADING_CALENDAR
    for c in schema:
        assert c["description"], c["name"]
    # 取り込みのテーブルではない（Terraform は infra/prod/calendar.tf で作る）
    assert "trading_calendar" not in load(NEW / "tables.json")["tables"]


# JSCC の VaR パラメータ（#164）。tables.json には入れない（Terraform は infra/prod/jscc.tf）
JSCC_TABLES = {
    "jscc_var_scenario": "business_date",
    "jscc_var_scenario_set": "business_date",
    "jscc_var_parameter": "business_date",
    "jscc_ingest_runs": "business_date",
}


def _all_columns(schema):
    for c in schema:
        yield c
        yield from _all_columns(c.get("fields", []))


@pytest.mark.parametrize("table", sorted(JSCC_TABLES))
def test_jscc_tables(table):
    schema = load(NEW / f"{table}.json")
    for c in _all_columns(schema):
        assert c["description"], c["name"]
        assert c["mode"] in ("NULLABLE", "REQUIRED", "REPEATED"), c["name"]
    # パーティションの列は DATE REQUIRED
    assert cols(schema)[JSCC_TABLES[table]] == ("DATE", "REQUIRED")
    assert table not in load(NEW / "tables.json")["tables"]


def test_jscc_scenario_columns():
    c = cols(load(NEW / "jscc_var_scenario.json"))
    assert c["pnl"] == ("INTEGER", "REPEATED")
    assert c["strike_price"] == ("NUMERIC", "NULLABLE")
    assert c["contract_period_code"] == ("STRING", "REQUIRED")  # 週次は 7 桁（YYYYMMW）
    # クラスタの列（infra/prod/jscc.tf）
    for name in ("product_code", "contract_period_code", "put_call_type", "strike_price"):
        assert name in c
    scenario_set = {s["name"]: s for s in load(NEW / "jscc_var_scenario_set.json")}
    assert scenario_set["scenarios"]["type"] == "RECORD" and scenario_set["scenarios"]["mode"] == "REPEATED"
    assert [f["name"] for f in scenario_set["scenarios"]["fields"]] == ["scenario_id", "scenario_type"]
