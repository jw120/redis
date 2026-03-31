"""Test ping command is handled."""

from logging import StreamHandler, getLogger
from multiprocessing import Process
from subprocess import run

from .base import Config
from .master import RedisMasterServer

SERVER_PORT = 6399
SERVER_CONFIG: Config = {"dir": "xxx", "dbfilename": "yyy"}

logger = getLogger(__name__)
logger.addHandler(StreamHandler())


def start() -> None:
    """Start our server."""
    RedisMasterServer(SERVER_PORT, SERVER_CONFIG, logger).run()


server_process = Process(target=start)
server_process.start()


def test_ping() -> None:
    """Test ping command."""
    completed = run(["redis-cli", "-p", str(SERVER_PORT), "PING"], check=True)
    assert completed.stdout == b"PONG"
