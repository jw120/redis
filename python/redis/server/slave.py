"""Slave server implementation."""

import asyncio
from enum import Enum, auto
from logging import Logger

from redis.resp import (
    Command,
    Incomplete,
    String,
    array,
    bulk,
    parse,
)
from redis.server.base import READ_BUFFER_SIZE, Address, Config, RedisBaseServer


class HandshakeState(Enum):
    """Current state of initial handshake with master server."""

    INITIAL_SENT_PING = auto()
    INITIAL_SENT_RC1 = auto()
    INITIAL_SENT_RC2 = auto()
    INITIAL_SENT_PSYNC = auto()
    INITIAL_EXPECT_FILE = auto()
    FINISHED = auto()


class RedisSlaveServer(RedisBaseServer):
    """Master server for Redis requests."""

    def __init__(
        self, port: int | None, config: Config, master: Address, logger: Logger | None
    ) -> None:
        super().__init__(port, config, logger)
        self.master_address: Address = master
        self.offset: int = 0  # Byte count of commands received on replication connection

    async def replication_connection(self) -> None:
        """Open and manage replication connection with master server."""
        master_host, master_port = self.master_address
        reader, writer = await asyncio.open_connection(master_host, master_port)
        self.log_info("master_conn", "opened")
        peer: Address = writer.transport.get_extra_info("socket").getpeername()
        self.log_info("master_conn to peer", peer)

        writer.write(array([b"PING"]))
        await writer.drain()
        state = HandshakeState.INITIAL_SENT_PING
        self.log_info("master_conn", "sent ping")

        data: bytes = b""
        force_read: bool = False
        while True:
            if not data or force_read:
                data += await reader.read(READ_BUFFER_SIZE)
                if not data:
                    break
            force_read = False
            self.log_info("master_conn", data)
            parsed = parse(data)
            self.log_info("parsed", parsed)
            match parsed:
                case Incomplete():
                    force_read = True
                case Command(value=cmd, length=l, remainder=r):
                    for b in await self.handle_replication_command(cmd, writer):
                        self.log_info(f"sending ({len(b)})", b)
                        writer.write(b)
                        await writer.drain()
                    data = r
                    self.offset += l
                case String(value=s, remainder=r):
                    state, b = await self.handle_replication_string(state, s)
                    writer.write(b)
                    await writer.drain()
                    data = r
                case other:
                    raise ValueError(f"Expected command, got f{other}")
        self.log_info("master_connection", "end")

    async def handle_replication_string(
        self, state: HandshakeState, s: bytes
    ) -> tuple[HandshakeState, bytes]:
        """Handle a string during replication connection."""
        self.log_info("handle_str_master", s)
        self.log_info("state", state)
        match state:
            case HandshakeState.INITIAL_SENT_PING:
                assert s == b"PONG"
                return (
                    HandshakeState.INITIAL_SENT_RC1,
                    array([b"REPLCONF", b"listening-port", bytes(str(self.port), "ascii")]),
                )
            case HandshakeState.INITIAL_SENT_RC1:
                assert s == b"OK"
                return (HandshakeState.INITIAL_SENT_RC2, array([b"REPLCONF", b"capa", b"psync2"]))
            case HandshakeState.INITIAL_SENT_RC2:
                assert s == b"OK"
                return (HandshakeState.INITIAL_SENT_PSYNC, array([b"PSYNC", b"?", b"-1"]))
            case HandshakeState.INITIAL_SENT_PSYNC:
                [w0, repl_id, w2] = s.split()
                assert w0 == b"FULLRESYNC"
                assert w2 == b"0"
                self.log_info("fullresync", repl_id[:8])
                return (HandshakeState.INITIAL_EXPECT_FILE, b"")
            case HandshakeState.INITIAL_EXPECT_FILE:
                return (HandshakeState.FINISHED, b"")
            case HandshakeState.FINISHED:
                raise ValueError("Unexpected extra string.")

    async def handle_replication_command(
        self,
        command: list[bytes],
        writer: asyncio.StreamWriter,
    ) -> list[bytes]:
        """Parse and provide response for commands from replication connection."""
        self.log_info("slave handle_r_command", command)
        if (
            len(command) == 3
            and command[0].upper() == b"REPLCONF"
            and command[1].upper() == b"GETACK"
            and command[2] == b"*"
        ):
            return [array([b"REPLCONF", b"ACK", bytes(str(self.offset), "ascii")])]
        response = await self.handle_command(command, writer)
        if command[0].upper() in {b"SET", b"PING"}:
            self.log_info("response suppressed", response)
            return []
        return response

    async def handle_command(
        self,
        command: list[bytes],
        writer: asyncio.StreamWriter,
    ) -> list[bytes]:
        """Parse and provide response for commands."""
        self.log_info("slave handle_command", command)
        match [command[0].upper(), *command[1:]]:
            case [b"INFO", key]:
                assert key.upper() == b"REPLICATION"
                lines = [
                    b"# Replication",
                    b"role:slave",
                    b"master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
                    b"master_repl_offset:0",
                ]
                return [bulk(b"\r\n".join(lines))]
            case _:
                pass
        return await super().handle_command(command, writer)

    async def serve_forever(self) -> None:
        """Coroutine to run server indefinitely."""
        async with asyncio.TaskGroup() as tg:
            tg.create_task(super().serve_forever())
            tg.create_task(self.replication_connection())
