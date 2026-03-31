"""Connection handler."""

from time import sleep

from .server.base import Config, RedisBaseServer

TEST_CONFIG = Config(dir="/tmp/t", dbfilename="xyz")


def crlf(*bs: bytes) -> bytes:
    """Join bytes with CRLFs, adding trailing CRLFs if missing."""
    combined = b""
    for b in bs:
        combined += b
        if b[-2:] != b"\r\n":
            combined += b"\r\n"
    return combined


def bs(b: bytes) -> bytes:
    """Encode as a bulk-string."""
    return crlf(b"$" + bytes(str(len(b)), "ascii"), b)


def test_ping() -> None:
    """Test handling of PING command."""
    server = RedisBaseServer(None, TEST_CONFIG, None)
    cmd = [b"PING"]
    assert server.handle_command(cmd, None) == [b"+PONG\r\n"]


def test_echo() -> None:
    """Test handling of ECHO command."""
    server = RedisBaseServer(None, TEST_CONFIG, None)
    cmd = [b"ECHO", b"abc"]
    assert server.handle_command(cmd, None) == [bs(b"abc")]


def test_config() -> None:
    """Test handling of CONFIG command."""
    server = RedisBaseServer(None, TEST_CONFIG, None)
    cmd = [b"CONFIG", b"GET", b"dir"]
    assert server.handle_command(cmd, None) == [crlf(b"*2", bs(b"dir"), bs(b"/tmp/t"))]
    cmd = [b"CONFIG", b"GET", b"dbfilename"]
    assert server.handle_command(cmd, None) == [crlf(b"*2", bs(b"dbfilename"), bs(b"xyz"))]


def test_set_get() -> None:
    """Test handling of SET and GET commands."""
    server = RedisBaseServer(None, TEST_CONFIG, None)
    cmd = [b"KEYS", b"*"]
    assert server.handle_command(cmd, None) == [crlf(b"*0")]
    cmd = [b"SET", b"apple", b"red"]
    assert server.handle_command(cmd, None) == [b"+OK\r\n"]
    cmd = [b"SET", b"pear", b"purple"]
    assert server.handle_command(cmd, None) == [b"+OK\r\n"]
    cmd = [b"SET", b"pear", b"blue"]
    assert server.handle_command(cmd, None) == [b"+OK\r\n"]
    cmd = [b"GET", b"apple"]
    assert server.handle_command(cmd, None) == [bs(b"red")]
    cmd = [b"GET", b"pear"]
    assert server.handle_command(cmd, None) == [bs(b"blue")]
    cmd = [b"GET", b"orange"]
    assert server.handle_command(cmd, None) == [b"$-1\r\n"]
    cmd = [b"KEYS", b"*"]
    response = server.handle_command(cmd, None)
    expected1 = [crlf(b"*2", bs(b"apple"), bs(b"pear"))]
    expected2 = [crlf(b"*2", bs(b"pear"), bs(b"apple"))]
    assert response in (expected1, expected2)


def test_set_get_expiries() -> None:
    """Test handling of SET and GET commands with expiry times."""
    server = RedisBaseServer(None, TEST_CONFIG, None)
    cmd = [b"SET", b"apple", b"red", b"px", b"5"]
    assert server.handle_command(cmd, None) == [b"+OK\r\n"]
    cmd = [b"SET", b"pear", b"purple", b"px", b"1000"]
    assert server.handle_command(cmd, None) == [b"+OK\r\n"]
    cmd = [b"GET", b"apple"]
    assert server.handle_command(cmd, None) == [bs(b"red")]
    sleep(0.010)  # Sleep 10ms so apple expires
    cmd = [b"GET", b"apple"]
    assert server.handle_command(cmd, None) == [b"$-1\r\n"]
    cmd = [b"GET", b"pear"]
    assert server.handle_command(cmd, None) == [bs(b"purple")]


def test_info() -> None:
    """Test handling of INFO command."""
    server = RedisBaseServer(None, TEST_CONFIG, None)
    cmd = [b"info", b"replication"]
    [actual] = server.handle_command(cmd, None)
    assert b"role:master" in actual
