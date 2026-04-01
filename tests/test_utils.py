from datetime import datetime

from news_ingestor.utils import parse_datetime, safe_str, sanitize_for_bson, source_domain_from_url


def test_safe_str_handles_none_and_none_text() -> None:
    assert safe_str(None) == ""
    assert safe_str("None") == ""
    assert safe_str(" hello ") == "hello"


def test_parse_datetime_parses_rfc2822() -> None:
    parsed = parse_datetime("Wed, 02 Oct 2002 13:00:00 GMT")
    assert isinstance(parsed, datetime)
    assert parsed.tzinfo is not None


def test_sanitize_for_bson_rewrites_invalid_keys() -> None:
    value = {"$a.b": {"x.y": 1}}
    sanitized = sanitize_for_bson(value)
    assert "field_a_b" in sanitized
    assert "x_y" in sanitized["field_a_b"]


def test_source_domain_strips_www() -> None:
    assert source_domain_from_url("https://www.example.com/path") == "example.com"

