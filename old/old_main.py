"""CodeCrafters's Build your own Redis using python."""

from argparse import ArgumentParser
from logging import StreamHandler, getLogger
from typing import TYPE_CHECKING

from redis.server.master import RedisMasterServer
from redis.server.slave import RedisSlaveServer

if TYPE_CHECKING:
    from redis.server.base import Config


def main() -> None:
    """Run main program."""

    logger = getLogger(__name__)
    logger.addHandler(StreamHandler())
    logger.setLevel("INFO")

    parser = ArgumentParser(prog="cc-redis", description="CodeCrafters redis exercise")
    parser.add_argument("--dir", metavar="DIRECTORY", default=None)
    parser.add_argument("--dbfilename", metavar="FILENAME", default=None)
    parser.add_argument("--port", metavar="PORT", default="6379")
    parser.add_argument("--replicaof", metavar='"MASTER_HOST MASTER_PORT"', default=None)
    args = parser.parse_args()

    config: Config = {"dir": args.dir, "dbfilename": args.dbfilename}
    port: int = int(args.port)

    if args.replicaof is None:
        with RedisMasterServer(port, config, logger) as server:
            server.serve_forever()
    else:
        replica_parts = args.replicaof.split()
        assert len(replica_parts) == 2
        master = (replica_parts[0], int(replica_parts[1]))
        with RedisSlaveServer((int(args.port)), config, master, logger) as server:
            server.serve_forever()


if __name__ == "__main__":
    main()
