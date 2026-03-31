"""Tests for parser module."""

import pytest

from .resp import (
    Command,
    Incomplete,
    String,
    _argument,
    array,
    bulk,
    integer,
    parse,
    simple,
)


def test_parse_simple_string() -> None:
    """Test parsing simple string."""
    b = b"+ABC\r\ndef"
    assert parse(b) == String(value=b"ABC", remainder=b"def")


def test_parse_bulk_string_with_crlf() -> None:
    """Test parsing of a bulk string."""
    # With trailing crlf."""
    b = b"$5\r\nabcde\r\nREST"
    assert parse(b) == String(value=b"abcde", remainder=b"REST")
    # Without trailing crlf."""
    b = b"$4\r\nabcdMORE"
    assert parse(b) == String(value=b"abcd", remainder=b"MORE")
    # Incomplete
    b = b"$5\r\nabc"
    assert parse(b) == Incomplete()


def test_parse_command() -> None:
    """Test parsing of a command."""
    b = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\ngetack\r\n$1\r\n*\r\nQQ"
    assert parse(b) == Command(value=[b"REPLCONF", b"getack", b"*"], length=37, remainder=b"QQ")
    b = b"*3\r\n$3\r\nSET\r\n$1\r\nA\r\n$5\r\nhello\r\nXYZ"
    assert parse(b) == Command(value=[b"SET", b"A", b"hello"], length=31, remainder=b"XYZ")
    b = b"*3\r\n$3\r\nSET\r\n$1\r\nA\r\n$5\r\nhello\r\nXYZ\r\nA\r\n"
    assert parse(b) == Command(value=[b"SET", b"A", b"hello"], length=31, remainder=b"XYZ\r\nA\r\n")
    b = b"*3\r\n$3\r\nSET\r\n"
    assert parse(b) == Incomplete()


def test_argument() -> None:
    """Test argument parsing."""
    assert _argument(b"$3", b"SET") == b"SET"
    with pytest.raises(AssertionError):
        _argument(b"3", b"SET")  # Missing $
    with pytest.raises(AssertionError):
        _argument(b"$4", b"SET")  # Wrong count


def test_bulk() -> None:
    """Test bulk string encoding."""
    assert bulk(b"hello") == b"$5\r\nhello\r\n"
    assert bulk(b"") == b"$0\r\n\r\n"


def test_array() -> None:
    """Test array encoding."""
    assert array([b"dir", b"/tmp/redis-files"]) == b"*2\r\n$3\r\ndir\r\n$16\r\n/tmp/redis-files\r\n"


def test_simple() -> None:
    """Test simple string encoding."""
    assert simple(b"hello") == b"+hello\r\n"
    assert simple(b"") == b"+\r\n"


def test_integer() -> None:
    """Test integer encoding."""
    assert integer(0) == b":0\r\n"
    assert integer(1000) == b":1000\r\n"
