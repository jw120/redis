"""Base class server implementation."""

from logging import Logger
from pathlib import Path
from socket import socket
from socketserver import BaseRequestHandler, ThreadingTCPServer
from time import time_ns
from typing import Any, NotRequired, TypedDict

from redis.rdb import RDB, Store
from redis.resp import (
    array,
    bulk,
    decode_commands,
    null_bulk_string,
    simple,
)

# Type for an address-port pair
type Address = tuple[str, int]


# Type for configuration information
class Config(TypedDict):
    """Configuration information passed to server."""

    dir: NotRequired[str]
    dbfilename: NotRequired[str]


class RedisBaseServer(ThreadingTCPServer):
    """TCP server for redis requests.

    Base class to be sub-classed for Master or Slave.
    """

    allow_reuse_address = True

    def __init__(self, port: int | None, config: Config, logger: Logger | None) -> None:
        """Create a local server (port of None is for testing)."""

        self.logger = logger

        if port is None:
            self.port: int = -1
        else:
            self.port = port
            super().__init__(("localhost", port), RedisHandler)

        self.config: Config = config

        # Read database is one is given
        dbfilename: str | None = self.config.get("dbfilename")
        if dbfilename is None:
            self.store: Store = {}
        else:
            dbdir: str | None = self.config.get("dir")
            path = Path(dbfilename) if dbdir is None else Path(dbdir, dbfilename)
            try:
                self.store = RDB.from_file(path).store()
            except FileNotFoundError:
                self.store = {}

    def log_info(self, action: str, data: Any) -> None:  # noqa: ANN401
        """Log at information level."""
        if self.logger is not None:
            self.logger.info(
                f"{self.__class__.__name__.removeprefix("Redis"):12} - {action:20} - {data}"
            )

    def handle_data(self, data: bytes, connection: socket) -> None:
        """Handle one read of incoming data from connection."""
        self.log_info("handle_data", (connection.getsockname(), connection.getpeername()))
        for cmd in decode_commands(data):
            for b in self.handle_command(cmd, connection):
                self.log_info("sending", b)
                connection.sendall(b)

    def set(self, key: bytes, value: bytes, expiry_delta_ms: int | None = None) -> list[bytes]:
        """Set store value with optional expiry."""
        expiry_time_ns = (
            None if expiry_delta_ms is None else time_ns() + 1_000_000 * int(expiry_delta_ms)
        )
        self.store[key] = (value, expiry_time_ns)
        self.log_info("set", key)
        return [simple(b"OK")]

    def handle_command(self, command: list[bytes], _socket: socket | None) -> list[bytes]:
        """Parse and provide response for commands."""
        self.log_info("base handle_command", command)
        match [command[0].upper(), *command[1:]]:
            case [b"PING"]:
                return [simple(b"PONG")]
            case [b"ECHO", arg]:
                return [bulk(arg)]
            case [b"COMMAND", docs]:
                # Support this so can use redis-cli in interactive mode locally
                assert docs.upper() == b"DOCS"
                return [array([])]
            case [b"SET", key, value]:
                return self.set(key, value)
            case [b"SET", key, value, px, expiry_delta_ms]:
                assert px.upper() == b"PX"
                return self.set(key, value, int(expiry_delta_ms))
            case [b"GET", key]:
                value, expiry = self.store.get(key, (None, None))
                if expiry is not None and time_ns() > expiry:
                    value = None
                return [null_bulk_string if value is None else bulk(value)]
            case [b"CONFIG", get, key]:
                assert get.upper() == b"GET"
                config_value: str | None = self.config[str(key, "utf-8")]
                return [
                    null_bulk_string
                    if config_value is None
                    else array([key, bytes(config_value, "utf-8")])
                ]
            case [b"KEYS", _pattern]:
                return [array(list(self.store.keys()))]
            case _:
                raise ValueError(f"Unknown command received {command}")


class RedisHandler(BaseRequestHandler):
    """Handler for commands sent to our Redis server.

    Commands are passed back to the server's handle_command method.
    """

    def __init__(
        self, request: socket | None, client_address: Address, server: RedisBaseServer
    ) -> None:
        #  Hack to allow handler to see the non-base server type
        self.server: RedisBaseServer = server  # type: ignore
        if request is not None:  # For unit testing (no network access)
            super().__init__(request, client_address, server)

    def handle(self) -> None:
        """Handle connection sent to our Redis server."""
        self.server.log_info("handle", (self.request.getsockname(), self.request.getpeername()))
        handle_connection(self.request, self.server)


def handle_connection(connection: socket, server: RedisBaseServer) -> None:
    """Handle data from an open connection."""
    while data := connection.recv(1024):
        server.log_info("data", data)
        server.handle_data(data, connection)
