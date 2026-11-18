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
import pytest

from trino import sqltypes


@pytest.mark.parametrize(
    "type_str,expected",
    [
        ("BIGINT", "BIGINT"),
        ("varchar(10)", "varchar(10)"),
        ("ARRAY(BIGINT)", "ARRAY(BIGINT)"),
        ("array(map(varchar, decimal(4, 2)))", "ARRAY(MAP(varchar, DECIMAL(4, 2)))"),
        ("ROW(x INTEGER, y ARRAY(CHAR(3)))", "ROW(x INTEGER, y ARRAY(CHAR(3)))"),
        ("MAP(VARCHAR, ROW(a BIGINT, b TIMESTAMP(6) WITH TIME ZONE))",
         "MAP(VARCHAR, ROW(a BIGINT, b TIMESTAMP(6) WITH TIME ZONE))"),
        ("TIMESTAMP(3) WITH TIME ZONE", "TIMESTAMP(3) WITH TIME ZONE"),
        ("DECIMAL", "DECIMAL(38, 0)"),
        ("DECIMAL(38)", "DECIMAL(38, 0)"),
        ('ROW("first name" varchar, "last name" varchar)',
         'ROW("first name" varchar, "last name" varchar)'),
        ("ROW(TIMESTAMP WITH TIME ZONE, VARCHAR)", "ROW(TIMESTAMP WITH TIME ZONE, VARCHAR)"),
        ('ROW("time" BIGINT)', 'ROW("time" BIGINT)'),
    ]
)
def test_parse_type_round_trip(type_str, expected):
    parsed = sqltypes.parse_type(type_str)
    assert parsed.to_sql() == expected


def test_parse_type_keeps_nested_structure():
    parsed = sqltypes.parse_type("map(varchar, array(row(x bigint, y decimal(5, 2))))")
    assert isinstance(parsed, sqltypes.MapType)
    assert isinstance(parsed.value_type, sqltypes.ArrayType)
    row = parsed.value_type.item_type
    assert isinstance(row, sqltypes.RowType)
    assert [name for name, _ in row.fields] == ["x", "y"]
    assert row.fields[1][1] == sqltypes.DecimalType(5, 2)


def test_named_row_declaration_is_rendered():
    row = sqltypes.RowType([("x", "BIGINT"), ("y", sqltypes.ScalarType("VARCHAR"))])
    assert row.to_sql() == "ROW(x BIGINT, y VARCHAR)"
    assert row.attr_types == row.fields


def test_decimal_type_validates_precision_and_scale():
    with pytest.raises(ValueError):
        sqltypes.DecimalType(0, 0)
    with pytest.raises(ValueError):
        sqltypes.DecimalType(39, 0)
    with pytest.raises(ValueError):
        sqltypes.DecimalType(5, 6)


def test_positional_and_named_row_fields_can_be_mixed():
    row = sqltypes.RowType(["BIGINT", ("y", "VARCHAR")])
    assert row.to_sql() == "ROW(BIGINT, y VARCHAR)"


def test_invalid_type_declarations_are_rejected():
    with pytest.raises(TypeError):
        sqltypes.parse_type(42)
    with pytest.raises(ValueError):
        sqltypes.parse_type("")
    with pytest.raises(ValueError):
        sqltypes.parse_type("array(bigint")
    with pytest.raises(ValueError):
        sqltypes.parse_type("map(varchar)")
    with pytest.raises(ValueError):
        sqltypes.RowType([])


def test_type_specs_compare_by_sql_text():
    assert sqltypes.parse_type("array(bigint)") == sqltypes.ArrayType("BIGINT")
    assert sqltypes.parse_type("array(bigint)") != sqltypes.parse_type("array(integer)")
