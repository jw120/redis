"""Master server implementation."""

from logging import Logger
from socket import socket

from redis.resp import array, bulk, simple
from redis.server.base import Config, RedisBaseServer

EMPTY_RDS_FILE: bytes = bytes.fromhex(
    "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374"
    + "696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
)


class RedisMasterServer(RedisBaseServer):
    """Master server for Redis requests."""

    def __init__(self, port: int | None, config: Config, logger: Logger | None) -> None:
        super().__init__(port, config, logger)
        self.slave_listening_ports: list[int] = []
        self.slaves: list[socket] = []

    def propagate(self, command: list[bytes]) -> None:
        """Propagate command to slave server."""

        for slave in self.slaves:
            self.log_info("propagating to", slave)
            self.log_info("sendall", array(command))
            slave.sendall(array(command))

    def handle_command(self, command: list[bytes], socket: socket | None) -> list[bytes]:
        """Parse and provide response for commands."""
        self.log_info("handle_command", command)
        match [command[0].upper(), *command[1:]]:
            case [b"SET", key, value]:
                reply = self.set(key, value)
                self.propagate(command)
                return reply
            case [b"SET", key, value, px, expiry_delta_ms]:
                assert px.upper() == b"PX"
                reply = self.set(key, value, int(expiry_delta_ms))
                self.propagate(command)
            case [b"INFO", key]:
                assert key.upper() == b"REPLICATION"
                lines = [
                    b"# Replication",
                    b"role:master",
                    b"connected_slaves:0",
                    b"master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
                    b"master_repl_offset:0",
                ]
                return [bulk(b"\r\n".join(lines))]
            case [b"REPLCONF", b"listening-port", port]:
                if socket is not None:
                    self.slaves.append(socket)
                    self.slave_listening_ports.append(int(port))
                return [simple(b"OK")]
            case [b"REPLCONF", *_rest]:
                return [simple(b"OK")]
            case [b"PSYNC", b"?", b"-1"]:
                return [
                    simple(b"FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0"),
                    b"$88\r\n",
                    EMPTY_RDS_FILE,
                ]
            case _:
                pass
        return super().handle_command(command, socket)
