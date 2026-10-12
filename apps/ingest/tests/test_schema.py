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
