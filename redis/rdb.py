"""Handle RDB file reading."""

from dataclasses import dataclass
from pathlib import Path
from typing import Final, Self

type Position = int

# Type of our key-value store (with optional expiry time in nanoseconds)
type Store = dict[bytes, tuple[bytes, int | None]]

RDB_HEADER: Final[bytes] = b"REDIS0011"


@dataclass
class Database:
    """Hold database read from a RDB file."""

    index: int
    hash_table_size: int
    expires_hash_table_size: int
    hashes: dict[bytes, tuple[bytes, int | None]]


class RDB:
    """Represent an RDB file."""

    def __init__(self) -> None:
        self.data: bytes = b""
        self.pos: int = 0
        self.metadata: list[tuple[bytes, bytes]] = []

    @classmethod
    def from_file(cls, source: Path) -> Self:
        """Construct an RDB reader from a file."""
        r = cls()
        with open(source, mode="rb") as f:
            r.data = f.read(None)
        assert r.data[: len(RDB_HEADER)] == RDB_HEADER
        r.pos = len(RDB_HEADER)
        # Read metadata
        while r and r.peek_byte() == 0xFA:
            r.read_byte()
            key = r.read_string()
            value = r.read_string()
            r.metadata.append((key, value))
        return r

    @classmethod
    def from_bytes(cls, source: bytes) -> Self:
        """Construct an RDN reader from bytes (for testing purposes)."""
        r = cls()
        r.data = source
        return r

    def database(self) -> Database | None:
        """Read database from the file."""
        # If at the end, then return none
        if not self or self.peek_byte() == 0xFF:
            self.skip(9)
            return None
        # Database header and index
        assert self.read_byte() == 0xFE
        index = self.read_size()
        # Hash table sizes
        assert self.read_byte() == 0xFB
        hash_table_size = self.read_size()
        expires_hash_table_size = self.read_size()
        hashes: dict[bytes, tuple[bytes, int | None]] = {}
        for _ in range(hash_table_size):
            value_type = self.read_byte()
            expiry: int | None = None
            if value_type == 0xFC:  # Expiry in milliseconds
                expiry = self.read_little64() * 1_000_000
                value_type = self.read_byte()
            elif value_type == 0xFD:  # Expiry in seconds
                expiry = self.read_little32() * 1_000_000_000
                value_type = self.read_byte()
            assert value_type == 0
            key = self.read_string()
            value = self.read_string()
            hashes[key] = (value, expiry)
        return Database(
            index=index,
            hash_table_size=hash_table_size,
            expires_hash_table_size=expires_hash_table_size,
            hashes=hashes,
        )

    def store(self) -> Store:
        """Read all keys and values into a Store."""
        store: Store = {}
        while (d := self.database()) is not None:
            store |= d.hashes
        # for k, (v, x) in store.items():
        #     print(str(k, "ascii"), str(v, "ascii"), "" if x is None else hex(x))
        return store

    def __bool__(self) -> bool:
        """Test if there is any remaining data."""
        return self.pos < len(self.data)

    def read_byte(self) -> int:
        """Read one byte."""
        b = self.data[self.pos]
        self.pos += 1
        return b

    def peek_byte(self) -> int:
        """Read one byte without advancing the read position."""
        return self.data[self.pos]

    def skip(self, n: int) -> None:
        """Skip bytes."""
        self.pos += n
        assert self.pos <= len(self.data)

    def read_little16(self) -> int:
        """Read 16-bit little-endian integer."""
        return int.from_bytes([self.read_byte() for _ in range(2)], byteorder="little")

    def read_little32(self) -> int:
        """Read 32-bit little-endian integer."""
        return int.from_bytes([self.read_byte() for _ in range(4)], byteorder="little")

    def read_little64(self) -> int:
        """Read 64-bit little-endian integer."""
        return int.from_bytes([self.read_byte() for _ in range(8)], byteorder="little")

    def read_big32(self) -> int:
        """Read 32-bit big-endian integer."""
        return int.from_bytes([self.read_byte() for _ in range(4)], byteorder="big")

    def read_size_or_bytes(self) -> int | bytes:
        """Read size or size-encoded string."""
        size = self.read_byte()
        specifier = (size & 0b1100_0000) >> 6
        match specifier:
            case 0b00:
                return size
            case 0b01:
                return ((size & 0b0011_1111) << 8) + self.read_byte()
            case 0b10:
                return self.read_big32()
            case 0b11:
                match size:
                    case 0xC0:
                        return bytes(str(self.read_byte()), "ascii")
                    case 0xC1:
                        return bytes(str(self.read_little16()), "ascii")
                    case 0xC2:
                        return bytes(str(self.read_little32()), "ascii")
                    case 0xC3:
                        raise ValueError("LZF-compressed strings not handled.")
                    case _:
                        raise ValueError(f"Unknown string-encoding, {size}")
            case _:
                raise ValueError("Internal failure.")

    def read_size(self) -> int:
        """Read size."""
        s = self.read_size_or_bytes()
        assert isinstance(s, int)
        return s

    def read_string(self) -> bytes:
        """Read size-encoded string."""
        size = self.read_size_or_bytes()
        if isinstance(size, bytes):
            return size
        self.pos += size
        return self.data[self.pos - size : self.pos]


if __name__ == "__main__":
    RDB.from_file(Path("test_data/test.rdb"))
