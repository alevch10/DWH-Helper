import pytest
import json
import datetime as dt
from uuid import UUID
from app.etl.utils import convert_type

class TestConvertType:
    # ---------- string ----------
    @pytest.mark.parametrize("value, expected", [
        ("hello", "hello"),
        (123, "123"),
        (None, None),
    ])
    def test_string(self, value, expected):
        assert convert_type(value, "string") == expected

    # ---------- integer ----------
    @pytest.mark.parametrize("value, expected", [
        ("42", 42),
        (42, 42),
        (None, None),
    ])
    def test_integer_valid(self, value, expected):
        assert convert_type(value, "integer") == expected

    def test_integer_invalid(self):
        with pytest.raises(ValueError, match="Cannot convert value 'abc' to integer"):
            convert_type("abc", "integer")

    # ---------- float ----------
    @pytest.mark.parametrize("value, expected", [
        ("3.14", 3.14),
        (3.14, 3.14),
        (None, None),
    ])
    def test_float_valid(self, value, expected):
        assert convert_type(value, "float") == expected

    def test_float_invalid(self):
        with pytest.raises(ValueError, match="Cannot convert value 'xyz' to float"):
            convert_type("xyz", "float")

    # ---------- boolean ----------
    @pytest.mark.parametrize("value, expected", [
        (True, True),
        (False, False),
        ("true", True),
        ("false", False),
        (None, None),
    ])
    def test_boolean_valid(self, value, expected):
        assert convert_type(value, "boolean") == expected

    # ---------- datetime ----------
    @pytest.mark.parametrize("value, fmt, expected", [
        # ISO формат с T
        ("2026-06-01T12:00:00", None, dt.datetime(2026, 6, 1, 12, 0, 0)),
        ("2026-06-01T12:00:00Z", "iso", dt.datetime(2026, 6, 1, 12, 0, 0, tzinfo=dt.timezone.utc)),
        # старый формат с микросекундами
        ("2026-06-01 12:00:00.123456", None, dt.datetime(2026, 6, 1, 12, 0, 0, 123456)),
        # формат AppMetrica без микросекунд
        ("2026-06-01 12:00:00", None, dt.datetime(2026, 6, 1, 12, 0, 0)),
        (None, None, None),
    ])
    def test_datetime_valid(self, value, fmt, expected):
        assert convert_type(value, "datetime", fmt) == expected

    def test_datetime_invalid(self):
        with pytest.raises(ValueError, match="Cannot convert value 'bad_date' to datetime"):
            convert_type("bad_date", "datetime")

    # ---------- json ----------
    def test_json_already_valid_string(self):
        valid = '{"a":1}'
        assert convert_type(valid, "json") == valid

    def test_json_fix_double_escaped_quotes(self):
        # входящая строка с \\"
        bad = '{"key": "value with \\"quotes\\""}'
        result = convert_type(bad, "json")
        # Должно стать валидным JSON
        import json
        try:
            parsed = json.loads(result)
            assert parsed == {"key": 'value with "quotes"'}
        except json.JSONDecodeError:
            pytest.fail("Не удалось исправить двойное экранирование")

    def test_json_dict_to_string(self):
        data = {"x": 1}
        res = convert_type(data, "json")
        assert json.loads(res) == data

    def test_json_list_to_string(self):
        data = [1, 2, 3]
        res = convert_type(data, "json")
        assert json.loads(res) == data

    def test_json_integer_to_string(self):
        assert convert_type(123, "json") == "123"

    # ---------- array ----------
    @pytest.mark.parametrize("value, expected", [
        ([1,2], [1,2]),
        (42, [42]),
        (None, None),
    ])
    def test_array(self, value, expected):
        assert convert_type(value, "array") == expected

    # ---------- inet ----------
    @pytest.mark.parametrize("value, expected", [
        ("192.168.1.1", "192.168.1.1/32"),
        ("::1", "::1"),
        # IPv6 в квадратных скобках (Amplitude)
        ("[0:0:0:0:0:0:0:1]", "::1"),
        ("[::1]", "::1"),
        (None, None),
    ])
    def test_inet_valid(self, value, expected):
        assert convert_type(value, "inet") == expected

    def test_inet_invalid(self):
        with pytest.raises(ValueError, match="Cannot convert value 'not_an_ip' to inet"):
            convert_type("not_an_ip", "inet")

    # ---------- uuid ----------
    @pytest.mark.parametrize("value, expected", [
        ("550e8400-e29b-41d4-a716-446655440000", "550e8400-e29b-41d4-a716-446655440000"),
        (None, None),
    ])
    def test_uuid_valid(self, value, expected):
        assert convert_type(value, "uuid") == expected

    def test_uuid_invalid(self):
        with pytest.raises(ValueError, match="Cannot convert value 'not-a-uuid' to uuid"):
            convert_type("not-a-uuid", "uuid")