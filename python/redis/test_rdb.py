"""Tests for parser module."""

from pathlib import Path

from .rdb import RDB


def test_size() -> None:
    """Test reading of sizes."""
    assert RDB.from_bytes(b"\x0a").read_size() == 10
    assert RDB.from_bytes(b"\x42\xbc").read_size() == 700
    assert RDB.from_bytes(b"\x80\x00\x00\x42\x68").read_size() == 17000


def test_string() -> None:
    """Test reading of size-encoded strings."""
    b = RDB.from_bytes(b"\x09\x72\x65\x64\x69\x73\x2d\x76\x65\x72").read_string()
    assert b == b"redis-ver"
    b = RDB.from_bytes(b"\x06\x36\x2e\x30\x2e\x31\x36").read_string()
    assert b == b"6.0.16"
    b = RDB.from_bytes(b"\xc0\x7b").read_string()
    assert b == b"123"
    b = RDB.from_bytes(b"\xc1\x39\x30").read_string()
    assert b == b"12345"
    b = RDB.from_bytes(b"\xc2\x87\xd6\x12\x00").read_string()
    assert b == b"1234567"


def test_sample_database() -> None:
    """Test reading database from sample file."""
    r = RDB.from_file(Path("integration/read_sample.rdb"))

    # Check metadata
    metadata_keys = [k for k, _v in r.metadata]
    assert metadata_keys == [b"redis-ver", b"redis-bits", b"ctime", b"used-mem", b"aof-base"]
    metadata_values = [v for _k, v in r.metadata]
    assert metadata_values[:2] == [b"7.2.6", b"64"]

    # Check reading a database
    d = r.database()
    assert d is not None
    assert d.index == 0
    assert d.hash_table_size == 2
    assert d.expires_hash_table_size == 0
    assert d.hashes[b"mykey"] == (b"myval", None)
    assert d.hashes[b"anotherkey"] == (b"42", None)
    assert len(d.hashes) == 2

    # Check no more data
    assert r.database() is None
    assert not r


def test_sample_store() -> None:
    """Test reading store from sample file."""
    store = RDB.from_file(Path("integration/read_sample.rdb")).store()
    assert store[b"mykey"] == (b"myval", None)
    assert store[b"anotherkey"] == (b"42", None)
    assert len(store) == 2


# def test_missing_database() -> None:
#     """Test reading from a missing file."""
#     r = RDB.from_file(Path("NO-SUCH_FILE"))
#     assert r.data == b""
#     assert r.pos == 0
#     assert r.metadata == []
