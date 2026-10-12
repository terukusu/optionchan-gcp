from datetime import UTC, date, datetime

import pytest

from optionchan_ingest import settings
from optionchan_ingest.bq_proto import (
    load_schema,
    message_class,
    proto_to_rows,
    rows_to_proto,
    schema_to_descriptor,
)
from optionchan_ingest.parser import jst

SCHEMA = [
    {"name": "s", "type": "STRING", "mode": "REQUIRED"},
    {"name": "i", "type": "INTEGER"},
    {"name": "f", "type": "FLOAT"},
    {"name": "b", "type": "BOOLEAN"},
    {"name": "ts", "type": "TIMESTAMP"},
    {"name": "dt", "type": "DATETIME"},
    {"name": "d", "type": "DATE"},
]


def roundtrip(rows, schema=SCHEMA):
    desc = schema_to_descriptor(schema, "T")
    return proto_to_rows(desc, schema, rows_to_proto(desc, schema, rows))


def test_roundtrip_seven_types():
    row = {
        "s": "日経",
        "i": -1234567890123,
        "f": 24.5,
        "b": False,
        "ts": datetime(2026, 10, 12, 1, 46, 0, 123456, tzinfo=UTC),
        "dt": jst(2026, 10, 10, 5, 9),
        "d": date(2026, 11, 12),
    }
    assert roundtrip([row]) == [row]


def test_none_is_unset():
    assert roundtrip([{"s": "x"}]) == [{"s": "x", "i": None, "f": None, "b": None, "ts": None, "dt": None, "d": None}]
    # 0 / False は None と区別される
    assert roundtrip([{"s": "", "i": 0, "b": False}])[0]["i"] == 0


def test_encoding_values():
    desc = schema_to_descriptor(SCHEMA, "T")
    cls = message_class(desc)
    row = {
        "s": "x",
        "ts": jst(1970, 1, 1, 9, 0),
        "dt": datetime(2026, 10, 12, 1, 46, tzinfo=UTC),
        "d": date(1970, 1, 2),
        "f": 3,
    }
    proto = rows_to_proto(desc, SCHEMA, [row])
    msg = cls.FromString(proto.serialized_rows[0])
    assert msg.ts == 0  # UNIX マイクロ秒
    assert msg.dt == "2026-10-12 10:46:00"  # JST の壁時計
    assert msg.d == 1  # エポックからの日数
    assert msg.f == 3.0


def test_required_none_raises():
    desc = schema_to_descriptor(SCHEMA, "T")
    with pytest.raises(ValueError):
        rows_to_proto(desc, SCHEMA, [{"s": None}])


def test_unknown_column_raises():
    desc = schema_to_descriptor(SCHEMA, "T")
    with pytest.raises(ValueError):
        rows_to_proto(desc, SCHEMA, [{"s": "x", "nope": 1}])


def test_naive_datetime_raises():
    desc = schema_to_descriptor(SCHEMA, "T")
    with pytest.raises(ValueError):
        rows_to_proto(desc, SCHEMA, [{"s": "x", "ts": datetime(2026, 10, 12)}])


def test_descriptor_labels():
    desc = schema_to_descriptor(SCHEMA, "T")
    labels = {f.name: f.label for f in desc.field}
    assert labels["s"] == 2  # LABEL_REQUIRED
    assert labels["i"] == 1  # LABEL_OPTIONAL


@pytest.mark.parametrize("table", settings.DATA_TABLES + (settings.RUNS_TABLE,))
def test_real_schemas_build_descriptor(table):
    schema = load_schema(settings.DEFAULT_SCHEMA_DIR, table)
    desc = schema_to_descriptor(schema, table)
    assert [f.name for f in desc.field] == [c["name"] for c in schema]
    message_class(desc)


def test_message_class_is_cached():
    a = message_class(schema_to_descriptor(SCHEMA, "T"))
    b = message_class(schema_to_descriptor(SCHEMA, "T"))
    assert a is b
    other = message_class(schema_to_descriptor(SCHEMA[:2], "T"))
    assert other is not a
