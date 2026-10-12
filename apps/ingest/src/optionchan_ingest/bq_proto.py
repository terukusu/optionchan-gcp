"""BigQuery のスキーマ JSON から Storage Write API 用の proto を作る（純粋関数）。

型の対応（Storage Write API の proto 型対応表に従う）:
STRING→string、INTEGER→int64、FLOAT→double、BOOLEAN→bool、
TIMESTAMP→int64（UNIX エポックからのマイクロ秒）、DATETIME→string（JST の壁時計 `YYYY-MM-DD HH:MM:SS`）、
DATE→int32（1970-01-01 からの日数）。REQUIRED→LABEL_REQUIRED、それ以外→LABEL_OPTIONAL（proto2）
"""

import functools
import json
from datetime import UTC, date, datetime
from pathlib import Path

from google.cloud.bigquery_storage_v1 import types
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

from .parser import JST

_FD = descriptor_pb2.FieldDescriptorProto
PROTO_TYPES = {
    "STRING": _FD.TYPE_STRING,
    "INTEGER": _FD.TYPE_INT64,
    "FLOAT": _FD.TYPE_DOUBLE,
    "BOOLEAN": _FD.TYPE_BOOL,
    "TIMESTAMP": _FD.TYPE_INT64,
    "DATETIME": _FD.TYPE_STRING,
    "DATE": _FD.TYPE_INT32,
}
_EPOCH_DATE = date(1970, 1, 1)
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def load_schema(schema_dir: str | Path, table: str) -> list[dict]:
    with open(Path(schema_dir) / f"{table}.json", encoding="utf-8") as f:
        return json.load(f)


def schema_to_descriptor(schema: list[dict], name: str = "Row") -> descriptor_pb2.DescriptorProto:
    desc = descriptor_pb2.DescriptorProto(name=name)
    for number, col in enumerate(schema, start=1):
        if col["type"] not in PROTO_TYPES:
            raise ValueError(f"unsupported type {col['type']} for {col['name']}")
        required = col.get("mode", "NULLABLE") == "REQUIRED"
        desc.field.add(
            name=col["name"],
            number=number,
            type=PROTO_TYPES[col["type"]],
            label=_FD.LABEL_REQUIRED if required else _FD.LABEL_OPTIONAL,
        )
    return desc


def message_class(descriptor: descriptor_pb2.DescriptorProto):
    """DescriptorProto から動的にメッセージクラスを作る。

    同じ記述子ならキャッシュしたクラスを返す（毎分プールとクラスを作り直すと、protobuf の実装によっては
    クラスが解放されずメモリが増えるため。テーブル 4 つ分しか作らない）
    """
    return _message_class(descriptor.SerializeToString(deterministic=True))


@functools.lru_cache(maxsize=32)
def _message_class(serialized: bytes):
    descriptor = descriptor_pb2.DescriptorProto.FromString(serialized)
    pool = descriptor_pool.DescriptorPool()
    file_proto = descriptor_pb2.FileDescriptorProto(name=f"{descriptor.name}.proto", syntax="proto2")
    file_proto.message_type.add().CopyFrom(descriptor)
    pool.Add(file_proto)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName(descriptor.name))


def _require_aware(value: datetime, col: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{col}: naive datetime")


def encode_value(bq_type: str, col: str, value):
    if bq_type == "TIMESTAMP":
        _require_aware(value, col)
        delta = value - _EPOCH
        return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    if bq_type == "DATETIME":
        _require_aware(value, col)
        return value.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")
    if bq_type == "DATE":
        if isinstance(value, datetime):
            raise ValueError(f"{col}: DATE には date を渡す")
        return (value - _EPOCH_DATE).days
    if bq_type == "FLOAT":
        return float(value)
    if bq_type == "INTEGER":
        if isinstance(value, bool):
            raise ValueError(f"{col}: bool を INTEGER に入れない")
        return int(value)
    if bq_type == "BOOLEAN":
        return bool(value)
    return str(value)


def decode_value(bq_type: str, value):
    """encode_value の逆（往復テスト・再処理の確認用）"""
    if bq_type == "TIMESTAMP":
        return datetime.fromtimestamp(value // 1_000_000, tz=UTC).replace(microsecond=value % 1_000_000)
    if bq_type == "DATETIME":
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=JST)
    if bq_type == "DATE":
        return date.fromordinal(_EPOCH_DATE.toordinal() + value)
    return value


def rows_to_proto(descriptor: descriptor_pb2.DescriptorProto, schema: list[dict], rows: list[dict]) -> types.ProtoRows:
    """行（列名 → Python 値）を ProtoRows にする。None の列はセットしない。REQUIRED 列が None なら ValueError"""
    cls = message_class(descriptor)
    columns = {c["name"]: c for c in schema}
    proto_rows = types.ProtoRows()
    for i, row in enumerate(rows):
        unknown = set(row) - set(columns)
        if unknown:
            raise ValueError(f"row {i}: スキーマに無い列 {sorted(unknown)}")
        msg = cls()
        for name, col in columns.items():
            value = row.get(name)
            if value is None:
                if col.get("mode") == "REQUIRED":
                    raise ValueError(f"row {i}: REQUIRED 列 {name} が None")
                continue
            setattr(msg, name, encode_value(col["type"], name, value))
        proto_rows.serialized_rows.append(msg.SerializeToString())
    return proto_rows


def proto_to_rows(descriptor: descriptor_pb2.DescriptorProto, schema: list[dict], proto_rows) -> list[dict]:
    cls = message_class(descriptor)
    out = []
    for data in proto_rows.serialized_rows:
        msg = cls.FromString(data)
        row = {}
        for col in schema:
            name = col["name"]
            row[name] = decode_value(col["type"], getattr(msg, name)) if msg.HasField(name) else None
        out.append(row)
    return out
