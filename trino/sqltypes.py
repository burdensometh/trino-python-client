# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Declarative Trino SQL types.

Some query parameter values do not carry enough information for Trino to
infer their type.  The obvious examples are empty arrays, empty maps and rows
whose fields are all ``NULL``::

    SELECT ?                       -- params=([],)
    SELECT ?                       -- params=({},)
    SELECT ?                       -- params=((None, None),)

The SQL literals ``ARRAY[]``, ``MAP()`` and ``ROW(NULL, NULL)`` are ambiguous
and the coordinator rejects them.  Callers can disambiguate such parameters
by supplying a type declaration through the ``typed`` argument of
:meth:`trino.dbapi.Cursor.execute`::

    from trino.sqltypes import ArrayType, MapType, RowType, DecimalType, ScalarType

    cur.execute(
        "SELECT ?",
        params=([],),
        typed=[ArrayType(ScalarType("BIGINT"))],
    )

Declarations can also be written as SQL type strings, which is what the
SQLAlchemy dialect does::

    cur.execute("SELECT ?", params=([],), typed=["array(bigint)"])

The classes in this module only describe a type.  Validation of the matching
Python value and the generation of a ``CAST`` expression is performed by the
DBAPI cursor.
"""
from __future__ import annotations

import re
from typing import Any
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple

__all__ = [
    "SqlType",
    "ScalarType",
    "DecimalType",
    "ArrayType",
    "MapType",
    "RowType",
    "Typed",
    "parse_type",
    "array_of",
    "map_of",
    "row_of",
    "decimal_type",
]

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DECIMAL_RE = re.compile(r"^\s*(\d+)\s*(?:,\s*(\d+)\s*)?$")


class SqlType:
    """Base class for declarative Trino types."""

    def to_sql(self) -> str:
        """Return the canonical Trino SQL representation of this type."""
        raise NotImplementedError()

    def __str__(self) -> str:
        return self.to_sql()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.to_sql()!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SqlType):
            return NotImplemented
        return self.to_sql().lower() == other.to_sql().lower()

    def __hash__(self) -> int:
        return hash(self.to_sql().lower())


class ScalarType(SqlType):
    """A primitive (non-container) Trino type such as ``BIGINT`` or ``VARCHAR(10)``."""

    def __init__(self, name: str) -> None:
        if not isinstance(name, str):
            raise TypeError(f"Scalar type name must be a string, got {type(name).__name__}")
        name = re.sub(r"\s+", " ", name.strip())
        if not name:
            raise ValueError("Scalar type name must not be empty")
        self.name = name

    def to_sql(self) -> str:
        return self.name


class DecimalType(SqlType):
    """``DECIMAL(precision, scale)`` with an explicitly declared precision."""

    def __init__(self, precision: int = 38, scale: int = 0) -> None:
        if isinstance(precision, bool) or not isinstance(precision, int):
            raise TypeError(f"DECIMAL precision must be an integer, got {type(precision).__name__}")
        if isinstance(scale, bool) or not isinstance(scale, int):
            raise TypeError(f"DECIMAL scale must be an integer, got {type(scale).__name__}")
        if not 1 <= precision <= 38:
            raise ValueError(f"DECIMAL precision must be between 1 and 38, got {precision}")
        if not 0 <= scale <= precision:
            raise ValueError(
                f"DECIMAL scale must be between 0 and the precision ({precision}), got {scale}"
            )
        self.precision = precision
        self.scale = scale

    def to_sql(self) -> str:
        return f"DECIMAL({self.precision}, {self.scale})"


class ArrayType(SqlType):
    """``ARRAY(item_type)``."""

    def __init__(self, item_type: "TypeSpec") -> None:
        self.item_type: SqlType = parse_type(item_type)

    def to_sql(self) -> str:
        return f"ARRAY({self.item_type.to_sql()})"


class MapType(SqlType):
    """``MAP(key_type, value_type)``."""

    def __init__(self, key_type: "TypeSpec", value_type: "TypeSpec") -> None:
        self.key_type: SqlType = parse_type(key_type)
        self.value_type: SqlType = parse_type(value_type)

    def to_sql(self) -> str:
        return f"MAP({self.key_type.to_sql()}, {self.value_type.to_sql()})"


class RowType(SqlType):
    """``ROW(name type, ...)``.  Field names are optional."""

    def __init__(self, fields: Iterable["FieldSpec"]) -> None:
        normalized: List[Tuple[Optional[str], SqlType]] = []
        for field in fields:
            if isinstance(field, (tuple, list)):
                if len(field) != 2:
                    raise ValueError(
                        "A ROW field must be either a type or a (name, type) pair, "
                        f"got {field!r}"
                    )
                name, field_type = field
                if name is not None and not isinstance(name, str):
                    raise TypeError(f"ROW field name must be a string, got {type(name).__name__}")
                normalized.append((name, parse_type(field_type)))
            else:
                normalized.append((None, parse_type(field)))
        if not normalized:
            raise ValueError("ROW type must declare at least one field")
        self.fields: List[Tuple[Optional[str], SqlType]] = normalized

    @property
    def attr_types(self) -> List[Tuple[Optional[str], SqlType]]:
        # Mirrors the attribute exposed by trino.sqlalchemy.datatype.ROW.
        return self.fields

    def to_sql(self) -> str:
        rendered = []
        for name, field_type in self.fields:
            if name is None:
                rendered.append(field_type.to_sql())
            else:
                rendered.append(f"{_quote_identifier_if_needed(name)} {field_type.to_sql()}")
        return f"ROW({', '.join(rendered)})"


class Typed:
    """Wrap a single parameter together with its type declaration.

    This is a convenience for callers that prefer to declare types inline
    instead of passing a parallel ``typed`` sequence::

        cur.execute("SELECT ?", params=(Typed([], "array(bigint)"),))
    """

    def __init__(self, value: Any, type: "TypeSpec") -> None:
        self.value = value
        self.type: SqlType = parse_type(type)

    def __repr__(self) -> str:
        return f"Typed(value={self.value!r}, type={self.type.to_sql()!r})"


# A type may be declared with one of the classes above or with a Trino SQL
# type string such as "array(map(varchar, bigint))".
TypeSpec = Any
FieldSpec = Any


def parse_type(type_spec: "TypeSpec") -> SqlType:
    """Convert a type declaration into a :class:`SqlType` instance."""
    if isinstance(type_spec, SqlType):
        return type_spec
    if isinstance(type_spec, str):
        return _parse_sql_type(type_spec)
    if isinstance(type_spec, type) and issubclass(type_spec, SqlType):
        return type_spec()
    raise TypeError(
        "A type declaration must be a trino.sqltypes.SqlType instance or a SQL type "
        f"string, got {type(type_spec).__name__}"
    )


def array_of(item_type: "TypeSpec") -> ArrayType:
    return ArrayType(item_type)


def map_of(key_type: "TypeSpec", value_type: "TypeSpec") -> MapType:
    return MapType(key_type, value_type)


def row_of(fields: Iterable["FieldSpec"]) -> RowType:
    return RowType(fields)


def decimal_type(precision: int = 38, scale: int = 0) -> DecimalType:
    return DecimalType(precision, scale)


def _parse_sql_type(type_str: str) -> SqlType:
    type_str = re.sub(r"\s+", " ", type_str.strip())
    if not type_str:
        raise ValueError("SQL type must not be empty")

    name, args, remainder = _split_call(type_str)
    normalized_name = name.lower()

    if remainder:
        # e.g. "timestamp(6) with time zone"
        if normalized_name in ("array", "map", "row", "decimal"):
            raise ValueError(f"Unexpected trailing text in SQL type {type_str!r}")
        if args is None:
            raise ValueError(f"Unexpected trailing text in SQL type {type_str!r}")
        return ScalarType(f"{name}({args}) {remainder}")

    if args is None:
        if normalized_name == "decimal":
            return DecimalType()
        return ScalarType(name)

    if normalized_name == "array":
        return ArrayType(_parse_single_argument(args, type_str))

    if normalized_name == "map":
        parts = _split_top_level(args)
        if len(parts) != 2:
            raise ValueError(f"MAP type requires exactly two arguments: {type_str!r}")
        return MapType(_parse_component(parts[0], type_str), _parse_component(parts[1], type_str))

    if normalized_name == "row":
        fields = [_parse_row_field(part, type_str) for part in _split_top_level(args)]
        return RowType(fields)

    if normalized_name == "decimal":
        match = _DECIMAL_RE.match(args)
        if not match:
            raise ValueError(f"Invalid DECIMAL type specification: {type_str!r}")
        precision = int(match.group(1))
        scale = int(match.group(2)) if match.group(2) is not None else 0
        return DecimalType(precision, scale)

    return ScalarType(f"{name}({args})")


def _parse_single_argument(args: str, type_str: str) -> SqlType:
    parts = _split_top_level(args)
    if len(parts) != 1:
        raise ValueError(f"ARRAY type requires exactly one argument: {type_str!r}")
    return _parse_component(parts[0], type_str)


def _parse_component(component: str, type_str: str) -> SqlType:
    component = component.strip()
    if not component:
        raise ValueError(f"Missing type argument in {type_str!r}")
    return _parse_sql_type(component)


def _parse_row_field(field: str, type_str: str) -> Tuple[Optional[str], SqlType]:
    field = field.strip()
    if not field:
        raise ValueError(f"Missing ROW field in {type_str!r}")

    if field.startswith('"'):
        name, remainder = _read_quoted_identifier(field)
        if remainder.strip():
            return name, _parse_component(remainder, type_str)
        raise ValueError(f"ROW field {field!r} does not declare a type")

    parts = field.split(None, 1)
    if len(parts) == 2 and _IDENTIFIER_RE.match(parts[0]) \
            and parts[0].lower() not in _TYPE_STARTING_WORDS:
        return parts[0], _parse_component(parts[1], type_str)
    return None, _parse_component(field, type_str)


# A type such as "TIMESTAMP WITH TIME ZONE" starts with a word that could be
# mistaken for a ROW field name.  A field whose first word is a known type
# name is therefore treated as an unnamed (positional) field.  A field name
# that collides with a type name can be written as a quoted identifier.
_TYPE_STARTING_WORDS = {
    "array",
    "bigint",
    "boolean",
    "char",
    "date",
    "decimal",
    "double",
    "int",
    "integer",
    "interval",
    "ipaddress",
    "json",
    "map",
    "numeric",
    "real",
    "row",
    "smallint",
    "time",
    "timestamp",
    "tinyint",
    "uuid",
    "varbinary",
    "varchar",
}


def _split_call(type_str: str) -> Tuple[str, Optional[str], str]:
    """Split ``name(args) rest`` into its three components.

    ``args`` is ``None`` when the type has no parenthesised arguments.
    """
    open_index = type_str.find("(")
    if open_index == -1:
        return type_str.strip(), None, ""

    depth = 0
    quote: Optional[str] = None
    index = open_index
    while index < len(type_str):
        char = type_str[index]
        if quote is not None:
            if char == "\\":
                index += 2
                continue
            if char == quote:
                if index + 1 < len(type_str) and type_str[index + 1] == quote:
                    index += 2
                    continue
                quote = None
        elif char in ("'", '"'):
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return (
                    type_str[:open_index].strip(),
                    type_str[open_index + 1:index].strip(),
                    type_str[index + 1:].strip(),
                )
        index += 1

    raise ValueError(f"Unbalanced parentheses in SQL type {type_str!r}")


def _split_top_level(text: str, delimiter: str = ",") -> List[str]:
    """Split ``text`` on ``delimiter`` ignoring quoted and nested sections."""
    parts: List[str] = []
    start = 0
    depth = 0
    quote: Optional[str] = None
    index = 0
    while index < len(text):
        char = text[index]
        if quote is not None:
            if char == "\\":
                index += 2
                continue
            if char == quote:
                if index + 1 < len(text) and text[index + 1] == quote:
                    index += 2
                    continue
                quote = None
        elif char in ("'", '"'):
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == delimiter and depth == 0:
            parts.append(text[start:index].strip())
            start = index + len(delimiter)
        index += 1
    parts.append(text[start:].strip())
    return parts


def _read_quoted_identifier(text: str) -> Tuple[str, str]:
    """Read a double-quoted identifier and return it with the remaining text."""
    if not text.startswith('"'):
        raise ValueError(f"Expected a quoted identifier in {text!r}")
    characters: List[str] = []
    index = 1
    while index < len(text):
        char = text[index]
        if char == "\\" and index + 1 < len(text):
            characters.append(text[index + 1])
            index += 2
            continue
        if char == '"':
            if index + 1 < len(text) and text[index + 1] == '"':
                characters.append('"')
                index += 2
                continue
            return "".join(characters), text[index + 1:]
        characters.append(char)
        index += 1
    raise ValueError(f"Unterminated quoted identifier in {text!r}")


def _quote_identifier_if_needed(name: str) -> str:
    # Words that start a type are quoted so that a field named "time" is not
    # confused with the TIME type when the declaration is parsed again.
    if _IDENTIFIER_RE.match(name) and name.lower() not in _TYPE_STARTING_WORDS:
        return name
    return '"' + name.replace('"', '""') + '"'
