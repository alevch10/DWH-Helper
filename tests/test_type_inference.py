import pytest
from app.data_scout.type_inference import infer_type


class TestPrimitiveTypes:
    """Простые встроенные типы Python."""

    def test_null(self):
        assert infer_type(None) == ("null", None)

    def test_boolean(self):
        assert infer_type(True) == ("boolean", None)
        assert infer_type(False) == ("boolean", None)

    def test_integer(self):
        assert infer_type(42) == ("integer", None)

    def test_float(self):
        assert infer_type(3.14159) == ("float", None)

    def test_dict(self):
        assert infer_type({"key": "value"}) == ("object", None)

    def test_list(self):
        assert infer_type([1, 2, 3]) == ("array", None)

    def test_string_fallback(self):
        assert infer_type("just some plain text") == ("string", None)


class TestUuidUlid:
    """UUID и ULID."""

    def test_uuid4(self):
        assert infer_type("f47ac10b-58cc-4372-a567-0e02b2c3d479") == ("uuid4", None)

    def test_uuid1(self):
        assert infer_type("6ba7b810-9dad-11d1-80b4-00c04fd430c8") == ("uuid1", None)

    def test_uuid_non_standard_version(self):
        # UUID версии 3,5 и т.д. – просто "uuid"
        assert infer_type("f47ac10b-58cc-3372-a567-0e02b2c3d479") == ("uuid", None)

    def test_ulid(self):
        assert infer_type("01GNB2S2FGN2P93QPXDNB4EN2R") == ("ulid", None)

    def test_ulid_invalid_character(self):
        # ULID допускает только Base32 символы; 'U' не разрешён
        assert infer_type("01GNB2S2FGN2P93QPXDNB4EN2U") == ("string", None)


class TestInternetIdentifiers:
    """Email, URL, Domain, IP-адреса и CIDR."""

    def test_email(self):
        assert infer_type("user@example.com") == ("email", None)

    def test_url_https(self):
        assert infer_type("https://example.com/path") == ("url", None)

    def test_url_http(self):
        assert infer_type("http://example.com") == ("url", None)

    def test_url_ftp(self):
        # Только http/https считается url
        assert infer_type("ftp://example.com") == ("string", None)

    def test_domain(self):
        assert infer_type("example.com") == ("domain", None)

    def test_domain_with_subdomain(self):
        assert infer_type("sub.example.com") == ("domain", None)

    def test_domain_with_hyphen(self):
        assert infer_type("my-domain.com") == ("domain", None)

    def test_ipv4(self):
        assert infer_type("192.168.1.1") == ("ipv4", None)

    def test_ipv6_full(self):
        assert infer_type("2001:0db8:85a3:0000:0000:8a2e:0370:7334") == ("ipv6", None)

    def test_ipv6_short(self):
        assert infer_type("2001:db8::1") == ("ipv6", None)

    def test_ipv6_loopback(self):
        assert infer_type("::1") == ("ipv6", None)

    def test_cidr_v4(self):
        assert infer_type("192.168.1.0/24") == ("cidr_v4", None)

    def test_cidr_v4_zero_mask(self):
        assert infer_type("0.0.0.0/0") == ("cidr_v4", None)

    def test_cidr_v6(self):
        assert infer_type("2001:db8::/32") == ("cidr_v6", None)

    def test_cidr_v6_big_mask(self):
        assert infer_type("::/0") == ("cidr_v6", None)


class TestS3Path:
    """S3-путь."""

    def test_s3_path(self):
        assert infer_type("s3://my-bucket/path/to/file") == ("s3_path", None)


class TestDsn:
    """Database Source Names (DSN)."""

    def test_dsn_postgres(self):
        assert infer_type("postgresql://user:pass@localhost:5432/db") == (
            "dsn_postgres",
            None,
        )

    def test_dsn_redis(self):
        assert infer_type("redis://localhost:6379/0") == ("dsn_redis", None)

    def test_dsn_mongo(self):
        assert infer_type("mongodb://localhost:27017") == ("dsn_mongo", None)


class TestPhoneNumber:
    """Телефонные номера."""

    def test_phone(self):
        assert infer_type("+1234567890") == ("phone", None)

    def test_phone_without_plus(self):
        # "1234567890" длиной 10 цифр — невалидный ISBN, поэтому станет epoch_int
        assert infer_type("1234567890") == ("epoch_int", None)

    def test_phone_with_spaces(self):
        # PhoneNumber может принимать пробелы, если библиотека позволяет
        result = infer_type("+1 234 567 890")
        assert result[0] in ("phone", "string")


class TestMacAddress:
    """MAC-адреса."""

    def test_mac_colon(self):
        assert infer_type("00:1A:2B:3C:4D:5E") == ("mac_address", None)

    def test_mac_dash(self):
        assert infer_type("00-1A-2B-3C-4D-5E") == ("mac_address", None)

    def test_mac_lowercase(self):
        assert infer_type("00-1a-2b-3c-4d-5e") == ("mac_address", None)

    def test_mac_mixed_separator(self):
        # Регулярка MAC разрешает смешанные разделители
        assert infer_type("00:1A-2B:3C-4D:5E") == ("mac_address", None)


class TestMimeType:
    """MIME-типы."""

    def test_mime(self):
        assert infer_type("application/json") == ("mime_type", None)

    def test_mime_with_parameter(self):
        # MimeType может принимать параметры, оставляем mime_type
        assert infer_type("text/html; charset=utf-8") == ("mime_type", None)


class TestCron:
    """Cron-выражения."""

    def test_cron_five_fields(self):
        assert infer_type("0 8 * * *") == ("cron", None)

    def test_cron_day_of_week(self):
        assert infer_type("0 8 * * 1") == ("cron", None)

    def test_cron_named(self):
        # @daily не распознается (нет 4 пробелов)
        assert infer_type("@daily") == ("string", None)


class TestColor:
    """Цвета."""

    def test_color_hex(self):
        assert infer_type("#ff0000") == ("color", None)

    def test_color_named(self):
        assert infer_type("red") == ("color", None)

    def test_color_rgb(self):
        assert infer_type("rgb(255,0,0)") == ("color", None)


class TestScriptCode:
    """ISO 15924 script codes."""

    def test_script(self):
        assert infer_type("Latn") == ("script_code", None)

    def test_script_lowercase(self):
        assert infer_type("latn") == ("script_code", None)

    def test_script_not_exists(self):
        # Несуществующий код
        assert infer_type("null") == ("string", None)
        assert infer_type("true") == ("string", None)


class TestLanguageCodes:
    """Языковые коды ISO 639."""

    def test_language_alpha2(self):
        assert infer_type("en") == ("language_alpha2", None)

    def test_language_alpha2_ru(self):
        assert infer_type("ru") == ("language_alpha2", None)

    def test_language_alpha3(self):
        assert infer_type("eng") == ("language_alpha3", None)

    def test_language_alpha3_spa(self):
        assert infer_type("spa") == ("language_alpha3", None)


class TestCountryCodes:
    """Коды стран ISO 3166."""

    def test_country_alpha2(self):
        assert infer_type("US") == ("country_alpha2", None)

    def test_country_alpha2_lowercase(self):
        assert infer_type("us") == ("country_alpha2", None)

    def test_country_alpha3(self):
        assert infer_type("USA") == ("country_alpha3", None)

    def test_country_alpha3_lowercase(self):
        assert infer_type("usa") == ("country_alpha3", None)


class TestCurrencyCodes:
    """Коды валют ISO 4217."""

    def test_currency(self):
        assert infer_type("USD") == ("currency", None)

    def test_currency_lowercase(self):
        assert infer_type("usd") == ("currency", None)


class TestDatetime:
    """Дата и время."""

    def test_datetime_iso(self):
        assert infer_type("2025-05-31T12:00:00Z") == ("datetime_iso", {"format": "iso"})

    def test_datetime_iso_offset(self):
        assert infer_type("2025-05-31T12:00:00+03:00") == (
            "datetime_iso",
            {"format": "iso"},
        )

    def test_datetime_iso_milliseconds(self):
        assert infer_type("2025-05-31T12:00:00.123Z") == (
            "datetime_iso",
            {"format": "iso"},
        )

    def test_datetime_custom(self):
        assert infer_type("2025-05-31 12:00:00") == (
            "datetime_custom",
            {"format": "custom"},
        )

    def test_datetime_custom_milliseconds(self):
        assert infer_type("2025-05-31 12:00:00.123") == (
            "datetime_custom",
            {"format": "custom"},
        )

    def test_datetime_invalid(self):
        # Невалидная дата
        assert infer_type("2025-13-01 12:00:00") == ("string", None)

    def test_pendulum_datetime(self):
        # Дата с 'T', но без часового пояса
        assert infer_type("2025-05-31T12:00:00") == ("pendulum_datetime", None)

    def test_pendulum_non_iso(self):
        # pendulum не вызывается, если нет 'T'
        assert infer_type("May 31, 2025") == ("string", None)


class TestIban:
    """IBAN."""

    def test_iban(self):
        assert infer_type("GB29NWBK60161331926819") == ("iban", None)


class TestIsbn:
    """ISBN."""

    def test_isbn13(self):
        assert infer_type("9788537809662") == ("isbn", None)

    def test_isbn10(self):
        assert infer_type("0306406152") == ("isbn", None)

    def test_isbn10_with_X(self):
        assert infer_type("080442957X") == ("isbn", None)


class TestAbaRouting:
    """ABA Routing Number."""

    def test_aba_routing(self):
        assert infer_type("122105155") == ("aba_routing", None)

    def test_aba_routing_invalid(self):
        # "123456789" скорее невалидный routing, станет epoch_int
        assert infer_type("123456789") == ("epoch_int", None)


class TestPaymentCard:
    """Номера платёжных карт."""

    def test_payment_card(self):
        assert infer_type("4111111111111111") == ("payment_card", None)

    def test_payment_card_short(self):
        # 11 цифр — не попадает в диапазон 12-19, будет epoch_int
        assert infer_type("41111111111") == ("epoch_int", None)


class TestEpoch:
    """Unix-время (epoch)."""

    def test_epoch_int(self):
        assert infer_type("1700000000") == ("epoch_int", None)

    def test_epoch_int_zero(self):
        assert infer_type("0") == ("epoch_int", None)

    def test_epoch_int_max(self):
        assert infer_type("253402300799") == ("epoch_int", None)

    def test_epoch_int_too_large(self):
        assert infer_type("253402300800") == ("string", None)

    def test_epoch_int_negative(self):
        assert infer_type("-1234567890") == ("string", None)

    def test_epoch_float(self):
        assert infer_type("1700000000.123") == ("epoch_float", None)

    def test_epoch_float_exponent(self):
        assert infer_type("1.7e9") == ("string", None)


class TestMongoObjectId:
    """MongoDB ObjectId."""

    def test_mongo_object_id(self):
        assert infer_type("507f191e810c19729de860ea") == ("mongo_object_id", None)

    def test_mongo_object_id_uppercase(self):
        assert infer_type("507F191E810C19729DE860EA") == ("mongo_object_id", None)


class TestSemanticVersion:
    """Семантические версии."""

    def test_semver(self):
        assert infer_type("1.2.3-alpha") == ("semantic_version", None)

    def test_semver_with_build(self):
        assert infer_type("1.0.0+build.1") == ("semantic_version", None)

    def test_semver_invalid_prefix(self):
        # Не начинается с X.Y.Z
        assert infer_type("v1.2.3") == ("string", None)


class TestTimezoneName:
    """Имена часовых поясов."""

    def test_timezone_name(self):
        assert infer_type("Europe/Moscow") == ("timezone_name", None)


class TestCoordinate:
    """Географические координаты."""

    def test_coordinate(self):
        assert infer_type("41.40338,2.17403") == ("coordinate", None)

    def test_coordinate_negative(self):
        assert infer_type("-41.40338,2.17403") == ("coordinate", None)

    def test_coordinate_with_spaces(self):
        # Пробелы не допускаются регуляркой
        assert infer_type("41.40338, 2.17403") == ("string", None)


class TestEdgeCases:
    """Пограничные и конфликтные случаи."""

    def test_string_looks_like_null(self):
        assert infer_type("null") == ("string", None)

    def test_string_looks_like_boolean(self):
        assert infer_type("true") == ("string", None)
        assert infer_type("false") == ("string", None)

    def test_integer_as_string(self):
        # Строка "123" становится epoch_int
        assert infer_type("123") == ("epoch_int", None)

    def test_float_as_string(self):
        # Строка "3.14" становится epoch_float
        assert infer_type("3.14") == ("epoch_float", None)

    def test_isbn_vs_epoch(self):
        # Валидный ISBN имеет приоритет перед epoch_int
        assert infer_type("0306406152") == ("isbn", None)
        # А невалидный ISBN становится epoch_int, если plausible
        assert infer_type("1700000000") == ("epoch_int", None)

    def test_aba_vs_epoch(self):
        # Невалидный routing number → epoch_int
        assert infer_type("123456789") == ("epoch_int", None)
