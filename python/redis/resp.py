"""Handle Redis serialization protocol."""

from dataclasses import dataclass
from itertools import batched
from typing import Final

CRLF: Final[bytes] = b"\r\n"


@dataclass
class Command:
    """Return value for parse success with a command."""

    value: list[bytes]
    length: int  # Bytes in the parsed command
    remainder: bytes


@dataclass
class Incomplete:
    """Return value for parse when more data is needed."""


@dataclass
class String:
    """Return value for parse success with a string."""

    value: bytes
    remainder: bytes


type Value = Command | String


def parse(b: bytes) -> Command | String | Incomplete:
    """Parse a REDIS serialisation protocol input.

    If provided an incomplete input, return incomplete to request
    additional data. Input data assumed to only end at the end of a line
    or in the middle of a bulk string.
    """
    assert b, "Cannot parse empty input"
    match b[0]:
        case 36:  # "$" bulk string
            c = b.index(CRLF, 1)
            count = int(b[1:c])  # Length of encoded string
            n = c + 2 + count  # Index after encoded string
            if n > len(b):
                return Incomplete()
            remainder = b[n:]
            if remainder[:2] == CRLF:
                remainder = remainder[2:]
            return String(value=b[c + 2 : c + 2 + count], remainder=remainder)
        case 42:  # "*"
            [parameters, *rest] = b.split(CRLF)
            n = int(parameters[1:])
            if len(rest) < 2 * n + 1:
                return Incomplete()
            remainder = CRLF.join(rest[2 * n :])
            #            length = 1 + len(str(n)) + 2 + sum(1 + len(str(count)) + 2 + len(content) + 2 for (count, content) in batched(rest[: 2 * n], 2))
            return Command(
                value=[
                    _argument(count, content)
                    for (count, content) in batched(rest[: 2 * n], 2, strict=True)
                ],
                length=len(b) - len(remainder),
                remainder=remainder,
            )
        case 43:  # "+"
            c = b.index(CRLF, 1)
            return String(value=b[1:c], remainder=b[c + 2 :])
        case _:
            raise ValueError(f"Unknown input in parse '{b}'")


def _argument(count: bytes, content: bytes) -> bytes:
    """Parse an argument (helper function)."""
    assert count[0] == ord("$")
    assert int(count[1:]) == len(content)
    return content


def bulk(data: bytes, *, skip_final_crlf: bool = False) -> bytes:
    """Encode the data as a bulk string."""
    return b"$" + bytes(str(len(data)), "ascii") + CRLF + data + (b"" if skip_final_crlf else CRLF)


# Used for a string that does not exist
null_bulk_string = b"$-1\r\n"


def array(data: list[bytes]) -> bytes:
    """Encode an array of data."""
    return b"*" + bytes(str(len(data)), "ascii") + CRLF + b"".join(bulk(d) for d in data)


def simple(data: bytes) -> bytes:
    """Encode the data as a simple string."""
    return b"+" + data + CRLF


def integer(i: int) -> bytes:
    """Encode the number as a RESP integer."""
    return b":" + bytes(str(i), "ascii") + CRLF
