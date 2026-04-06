"""Base class server implementation."""

import asyncio
from pathlib import Path
from time import time_ns
from typing import TYPE_CHECKING, Any, Final, NotRequired, TypedDict

from redis.rdb import RDB, Store
from redis.resp import (
    Command,
    Incomplete,
    array,
    bulk,
    null_bulk_string,
    parse,
    simple,
)

if TYPE_CHECKING:
    from logging import Logger

READ_BUFFER_SIZE: Final[int] = 1024


# Type for an address-port pair
type Address = tuple[str, int]


# Type for configuration information
class Config(TypedDict):
    """Configuration information passed to server."""

    dir: NotRequired[str]
    dbfilename: NotRequired[str]


class RedisBaseServer:
    """TCP server for redis requests.

    Base class to be sub-classed for Master or Slave.
    """

    def __init__(self, port: int | None, config: Config, logger: Logger | None) -> None:
        """Create a local server (port of None is for testing)."""

        self.config: Config = config
        self.logger: Logger | None = logger
        self.port: int = -1 if port is None else port
        self.store: Store = {}

        # Read database is one is given
        if (dbfilename := self.config.get("dbfilename")) is not None:
            dbdir: str | None = self.config.get("dir")
            path = Path(dbfilename) if dbdir is None else Path(dbdir, dbfilename)
            self.log_info("read", path)
            try:
                self.store = RDB.from_file(path).store()
                self.log_info("read", f"{len(self.store)} keys read")
            except FileNotFoundError:
                self.store = {}
                self.log_info("read", "file not found")
        else:
            self.log_info("read", "no db given")

    def log_info(self, action: str, data: Any) -> None:  # noqa: ANN401
        """Log at information level."""
        if self.logger is not None:
            self.logger.info(
                f"{self.__class__.__name__.removeprefix('Redis'):12} {self.port:4} - {action:20} - {data}"
            )

    async def handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle the full lifetime of a connection."""
        data: bytes = b""
        force_read = True
        while True:
            if not data or force_read:
                data += await reader.read(READ_BUFFER_SIZE)
                if not data:
                    break
            force_read = False
            self.log_info("handle_conn", data)
            parsed = parse(data)
            self.log_info("parsed", parsed)
            match parsed:
                case Incomplete():
                    force_read = True
                case Command(value=cmd, remainder=r):
                    for b in await self.handle_command(cmd, writer):
                        self.log_info(f"sending ({len(b)})", b)
                        writer.write(b)
                        await writer.drain()
                    data = r
                case other:
                    raise ValueError(f"Expected command, got f{other}")
        self.log_info("handle_connection", "end")

    def set(self, key: bytes, value: bytes, expiry_delta_ms: int | None = None) -> list[bytes]:
        """Set store value with optional expiry."""
        expiry_time_ns = (
            None if expiry_delta_ms is None else time_ns() + 1_000_000 * int(expiry_delta_ms)
        )
        self.store[key] = (value, expiry_time_ns)
        self.log_info("set", key)
        return [simple(b"OK")]

    async def handle_command(
        self,
        command: list[bytes],
        _writer: asyncio.StreamWriter,
    ) -> list[bytes]:
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

    async def serve_forever(self) -> None:
        """Coroutine to run server indefinitely."""
        server = await asyncio.start_server(self.handle_connection, None, self.port)
        self.log_info("serve_forever", self.port)
        async with server:
            await server.serve_forever()

    def run(self) -> None:
        """Run the server indefinitely."""
        asyncio.run(self.serve_forever())
