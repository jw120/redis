"""Main server implementation."""

from enum import Enum, auto
from logging import Logger
from socket import create_connection, socket

from redis.resp import (
    array,
    bulk,
    decode_commands,
    decode_leading_bulk_string,
    decode_leading_simple_string,
    simple,
)
from redis.server.base import Address, Config, RedisBaseServer, handle_connection


class State(Enum):
    """Current state of the slave server."""

    INITIAL_SENT_PING = auto()
    INITIAL_SENT_RC1 = auto()
    INITIAL_SENT_RC2 = auto()
    INITIAL_SENT_PSYNC = auto()
    INITIAL_EXPECT_FILE = auto()
    NORMAL = auto()


class RedisSlaveServer(RedisBaseServer):
    """Master server for Redis requests."""

    def __init__(
        self, port: int | None, config: Config, master: Address, logger: Logger | None
    ) -> None:
        super().__init__(port, config, logger)
        self.master_address: Address = master

        connection = create_connection(master)
        self.master_socket: socket = connection
        self.log_info("master conn", self.master_socket)
        connection.sendall(array([b"PING"]))
        self.state = State.INITIAL_SENT_PING
        handle_connection(connection, self)
        # recv_data = connection.recv(1024)
        # if not recv_data:
        #     return  # disconnected
        # assert recv_data == simple(b"PONG")
        # connection.sendall(array([b"REPLCONF", b"listening-port", bytes(str(self.port), "ascii")]))
        # recv_data = connection.recv(1024)
        # if not recv_data:
        #     return  # disconnected
        # assert recv_data == simple(b"OK")
        # connection.sendall(array([b"REPLCONF", b"capa", b"psync2"]))
        # recv_data = connection.recv(1024)
        # if not recv_data:
        #     return  # disconnected
        # assert recv_data == simple(b"OK")
        # connection.sendall(array([b"PSYNC", b"?", b"-1"]))
        # recv_data = connection.recv(1024)
        # if not recv_data:
        #     return  # disconnected
        # self.log_info("recv-last", recv_data)
        # (reply, after_reply) = decode_leading_simple_string(recv_data)
        # [w0, repl_id, w2] = reply.split()
        # assert w0 == b"FULLRESYNC"
        # assert w2 == b"0"
        # self.log_info("fullresync", repl_id[:8])
        # self.log_info("after_reply", after_reply)
        # if not after_reply:
        #     self.log_info("give up", "nothing after reply")
        #     return
        # (file_data, after_file_data) = decode_leading_bulk_string(after_reply)
        # self.log_info("got file", len(file_data))
        # for cmd in decode_commands(after_file_data):
        #     for b in self.handle_command(cmd, connection):
        #         connection.sendall(b)
        # while recv_data := connection.recv(1024):
        #     for cmd in decode_commands(recv_data):
        #         for b in self.handle_command(cmd, connection):
        #             connection.sendall(b)

    def handle_data(self, data: bytes, connection: socket) -> None:
        """Handle one read of incoming data from connection."""
        self.log_info(f"h_d {self.state}", (connection.getsockname(), connection.getpeername()))
        match self.state:
            case State.INITIAL_SENT_PING:
                assert data == simple(b"PONG")
                connection.sendall(
                    array([b"REPLCONF", b"listening-port", bytes(str(self.port), "ascii")])
                )
                self.state = State.INITIAL_SENT_RC1
            case State.INITIAL_SENT_RC1:
                assert data == simple(b"OK")
                connection.sendall(array([b"REPLCONF", b"capa", b"psync2"]))
                self.state = State.INITIAL_SENT_RC2
            case State.INITIAL_SENT_RC2:
                assert data == simple(b"OK")
                connection.sendall(array([b"PSYNC", b"?", b"-1"]))
                self.state = State.INITIAL_SENT_PSYNC
            case State.INITIAL_SENT_PSYNC:
                (reply, after_reply) = decode_leading_simple_string(data)
                [w0, repl_id, w2] = reply.split()
                assert w0 == b"FULLRESYNC"
                assert w2 == b"0"
                self.log_info("fullresync", repl_id[:8])
                self.log_info("after_reply", after_reply)
                self.state = State.INITIAL_EXPECT_FILE
                if after_reply:
                    self.handle_data(after_reply, connection)
            case State.INITIAL_EXPECT_FILE:
                file_data, after_file_data = decode_leading_bulk_string(data)
                self.log_info("got file", len(file_data))
                self.state = State.NORMAL
                if after_file_data:
                    self.handle_data(after_file_data, connection)
            case State.NORMAL:
                for cmd in decode_commands(data):
                    for b in self.handle_command(cmd, connection):
                        connection.sendall(b)

    def handle_command(self, command: list[bytes], socket: socket | None) -> list[bytes]:
        """Parse and provide response for commands."""
        assert self.state == State.NORMAL
        self.log_info("handle_command", command)
        from_master = socket == self.master_socket
        self.log_info("socket is", socket)
        self.log_info("master_socket is", self.master_socket)
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
        response = super().handle_command(command, socket)
        if from_master and command[0].upper() == b"SET":
            self.log_info("response suppressed", response)
            return []  # Suppress response
        return response
