"""The in-house MT (FIN) parser and builder.

CLAUDE.md specifies an in-house MT parser, so this is it: block splitting,
block 4 tag parsing, the header fields the Hub routes on, and the reverse for
outbound messages.

A FIN message is five blocks::

    {1:F01BANKGB2LAXXX0000000000}{2:I103BANKDEFFXXXXN}{3:{121:<uetr>}}{4:
    :20:REF123
    :32A:260928EUR1234,56
    -}{5:{CHK:0123456789AB}}

Block 3 field 121 carries the UETR, which is why the edge can key Kafka on it
without parsing block 4.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import Final

BLOCK_RE: Final = re.compile(r"\{(\d):")
TAG_RE: Final = re.compile(r"^:(\d{2}[A-Z]?):(.*)$")
UETR_TAG: Final = "121"

# Block 2 input:  I 103 <12-char LT address> <priority>[<monitoring><obsolescence>]
_BLOCK2_INPUT_RE: Final = re.compile(
    r"^I(?P<msg_type>\d{3})(?P<address>[A-Z0-9]{12})(?P<priority>[SNU])"
    r"(?P<monitoring>[1-3])?(?P<obsolescence>\d{3})?$"
)
# Block 2 output: O 103 <input time> <input ref 28> <output date/time> <priority>
_BLOCK2_OUTPUT_RE: Final = re.compile(
    r"^O(?P<msg_type>\d{3})(?P<input_time>\d{4})(?P<input_ref>[A-Z0-9]{28})"
    r"(?P<output_date>\d{6})(?P<output_time>\d{4})(?P<priority>[SNU])$"
)
_BLOCK1_RE: Final = re.compile(
    r"^(?P<app_id>[FAL])(?P<service_id>\d{2})(?P<address>[A-Z0-9]{12})"
    r"(?P<session>\d{4})(?P<sequence>\d{6})$"
)


class MtParseError(ValueError):
    """The message is not a well-formed FIN message."""


@dataclass(slots=True)
class MtHeader:
    """Blocks 1 and 2, flattened to what the Hub actually uses."""

    app_id: str = "F"
    service_id: str = "01"
    sender_lt: str = ""  # 12-char LT address from block 1
    session: str = "0000"
    sequence: str = "000000"
    direction: str = "I"  # I = input to SWIFT, O = output from SWIFT
    msg_type: str = ""
    receiver_lt: str = ""  # block 2 input only
    priority: str = "N"
    input_ref: str = ""  # block 2 output only
    output_date: str = ""
    output_time: str = ""

    @property
    def sender_bic(self) -> str:
        return _bic_from_lt(self.sender_lt)

    @property
    def receiver_bic(self) -> str:
        if self.receiver_lt:
            return _bic_from_lt(self.receiver_lt)
        # On an output message the receiver is us; the sender is in the input ref.
        return _bic_from_lt(self.input_ref[6:18]) if len(self.input_ref) >= 18 else ""

    @property
    def counterparty_bic(self) -> str:
        """Whoever is on the other end, whichever direction this message goes."""
        return self.receiver_bic if self.direction == "I" else self.sender_bic


@dataclass(slots=True)
class MtMessage:
    """A parsed FIN message."""

    header: MtHeader = field(default_factory=MtHeader)
    block3: dict[str, str] = field(default_factory=dict)
    tags: list[tuple[str, str]] = field(default_factory=list)
    block5: dict[str, str] = field(default_factory=dict)
    raw: str = ""

    # ------------------------------------------------------------ accessors
    @property
    def msg_type(self) -> str:
        return self.header.msg_type

    @property
    def uetr(self) -> str:
        return self.block3.get(UETR_TAG, "")

    def get(self, tag: str, default: str = "") -> str:
        """First value for ``tag``.

        ``get("50")`` matches any option — ``:50A:``, ``:50F:``, ``:50K:`` —
        because the Hub cares about the ordering customer, not which option
        the sender chose.
        """
        for name, value in self.tags:
            if name == tag or (len(tag) == 2 and name[:2] == tag):
                return value
        return default

    def get_all(self, tag: str) -> list[str]:
        return [
            value for name, value in self.tags if name == tag or (len(tag) == 2 and name[:2] == tag)
        ]

    def option(self, tag: str) -> str:
        """The option letter actually used, e.g. ``K`` for ``:50K:``."""
        for name, _ in self.tags:
            if len(tag) == 2 and name[:2] == tag and len(name) == 3:
                return name[2]
        return ""

    def has(self, tag: str) -> bool:
        return any(name == tag or (len(tag) == 2 and name[:2] == tag) for name, _ in self.tags)

    def sequences(self) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
        """Split an MT202 COV into sequence A and sequence B.

        Sequence A of an MT202 has no field 50, so the first ``:50a:`` starts
        sequence B. Only MT202 has sequences — in an MT103 field 50 is the
        ordering customer, so splitting there would cut the message in half.
        """
        if self.header.msg_type != "202":
            return list(self.tags), []
        for index, (name, _) in enumerate(self.tags):
            if name.startswith("50"):
                return self.tags[:index], self.tags[index:]
        return list(self.tags), []

    def sub(self, tags: Sequence[tuple[str, str]]) -> MtMessage:
        """A view of this message limited to ``tags`` (one COV sequence)."""
        return MtMessage(header=self.header, block3=self.block3, tags=list(tags), raw=self.raw)


# ---------------------------------------------------------------- parsing
def parse(raw: str | bytes) -> MtMessage:
    """Parse a FIN message. Raises :class:`MtParseError` on anything malformed."""
    text = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
    text = text.replace("\r\n", "\n").strip()
    if not text.startswith("{"):
        raise MtParseError("message does not start with a block")

    blocks = _split_blocks(text)
    if "1" not in blocks:
        raise MtParseError("missing block 1")
    if "4" not in blocks:
        raise MtParseError("missing block 4 (text block)")

    header = _parse_header(blocks)
    message = MtMessage(
        header=header,
        block3=_parse_subblocks(blocks.get("3", "")),
        tags=parse_block4(blocks["4"]),
        block5=_parse_subblocks(blocks.get("5", "")),
        raw=text,
    )
    if not message.tags:
        raise MtParseError("block 4 contains no fields")
    return message


def _split_blocks(text: str) -> dict[str, str]:
    """Split top-level ``{n:...}`` blocks, respecting nesting in 3 and 5."""
    blocks: dict[str, str] = {}
    pos = 0
    length = len(text)
    while pos < length:
        if text[pos] != "{":
            pos += 1
            continue
        match = BLOCK_RE.match(text, pos)
        if match is None:
            raise MtParseError(f"malformed block start at offset {pos}")
        number = match.group(1)
        depth = 1
        cursor = match.end()
        while cursor < length and depth:
            if text[cursor] == "{":
                depth += 1
            elif text[cursor] == "}":
                depth -= 1
            cursor += 1
        if depth:
            raise MtParseError(f"block {number} is not closed")
        blocks[number] = text[match.end() : cursor - 1]
        pos = cursor
    return blocks


def _parse_header(blocks: dict[str, str]) -> MtHeader:
    header = MtHeader()
    block1 = _BLOCK1_RE.match(blocks["1"].strip())
    if block1 is None:
        raise MtParseError(f"malformed block 1: {blocks['1']!r}")
    header.app_id = block1.group("app_id")
    header.service_id = block1.group("service_id")
    header.sender_lt = block1.group("address")
    header.session = block1.group("session")
    header.sequence = block1.group("sequence")

    block2 = blocks.get("2", "").strip()
    if not block2:
        raise MtParseError("missing block 2")
    if inp := _BLOCK2_INPUT_RE.match(block2):
        header.direction = "I"
        header.msg_type = inp.group("msg_type")
        header.receiver_lt = inp.group("address")
        header.priority = inp.group("priority")
    elif out := _BLOCK2_OUTPUT_RE.match(block2):
        header.direction = "O"
        header.msg_type = out.group("msg_type")
        header.input_ref = out.group("input_ref")
        header.output_date = out.group("output_date")
        header.output_time = out.group("output_time")
        header.priority = out.group("priority")
    else:
        raise MtParseError(f"malformed block 2: {block2!r}")
    return header


def _parse_subblocks(text: str) -> dict[str, str]:
    """Blocks 3 and 5 hold ``{tag:value}`` pairs."""
    out: dict[str, str] = {}
    for tag, value in re.findall(r"\{([A-Z0-9]+):([^}]*)\}", text):
        out[tag] = value
    return out


def parse_block4(text: str) -> list[tuple[str, str]]:
    """Parse the text block into ordered ``(tag, value)`` pairs.

    Values continue across lines until the next ``:NN:`` or the ``-}`` trailer,
    which is how fields like :50K: carry name and address.
    """
    body = text.strip()
    if body.endswith("-"):
        body = body[:-1]
    tags: list[tuple[str, str]] = []
    current: str | None = None
    lines: list[str] = []
    for line in body.replace("\r\n", "\n").split("\n"):
        match = TAG_RE.match(line)
        if match:
            if current is not None:
                tags.append((current, "\n".join(lines).strip()))
            current = match.group(1)
            lines = [match.group(2)]
        elif current is not None:
            lines.append(line)
    if current is not None:
        tags.append((current, "\n".join(lines).strip()))
    return tags


# --------------------------------------------------------------- building
def build(
    *,
    msg_type: str,
    sender_bic: str,
    receiver_bic: str,
    tags: Iterable[tuple[str, str]],
    uetr: str = "",
    block3_extra: dict[str, str] | None = None,
    priority: str = "N",
    session: str = "0000",
    sequence: str = "000000",
) -> str:
    """Build an input (bank to network) FIN message.

    Field 121 is written first in block 3, as the standard requires, so the
    receiving edge can find the UETR without scanning.
    """
    block1 = f"{{1:F01{_lt_from_bic(sender_bic)}{session}{sequence}}}"
    block2 = f"{{2:I{msg_type}{_lt_from_bic(receiver_bic)}{priority}}}"

    parts: list[str] = []
    if uetr:
        parts.append(f"{{{UETR_TAG}:{uetr}}}")
    for tag, value in (block3_extra or {}).items():
        if tag != UETR_TAG:
            parts.append(f"{{{tag}:{value}}}")
    block3 = f"{{3:{''.join(parts)}}}" if parts else ""

    body = "\n".join(f":{tag}:{value}" for tag, value in tags)
    block4 = "{4:\n" + body + "\n-}"
    return f"{block1}{block2}{block3}{block4}"


def rebuild(
    message: MtMessage, *, receiver_bic: str = "", tags: Sequence[tuple[str, str]] | None = None
) -> str:
    """Rebuild an outbound message from a parsed one, swapping the receiver."""
    return build(
        msg_type=message.msg_type,
        sender_bic=message.header.receiver_bic or message.header.sender_bic,
        receiver_bic=receiver_bic or message.header.counterparty_bic,
        tags=tags if tags is not None else message.tags,
        uetr=message.uetr,
        block3_extra={k: v for k, v in message.block3.items() if k != UETR_TAG},
    )


def peek_uetr(raw: str | bytes) -> str:
    """Pull field 121 out without a full parse.

    The gRPC edge does light checks only and must not parse; this reads block 3
    directly so a malformed block 4 cannot fail the delivery.
    """
    text = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
    match = re.search(r"\{" + UETR_TAG + r":([0-9a-fA-F-]{36})\}", text)
    return match.group(1).lower() if match else ""


def peek_msg_type(raw: str | bytes) -> str:
    """Read the message type from block 2 without a full parse."""
    text = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
    match = re.search(r"\{2:[IO](\d{3})", text)
    return f"MT{match.group(1)}" if match else ""


# ---------------------------------------------------------------- helpers
def _lt_from_bic(bic: str) -> str:
    """BIC8 or BIC11 to the 12-character logical terminal address.

    BIC8 gains the ``X`` LT identifier and an ``XXX`` branch; BIC11 has its LT
    identifier inserted after position 8.
    """
    clean = (bic or "").strip().upper()
    if len(clean) == 8:
        return clean + "X" + "XXX"
    if len(clean) == 11:
        return clean[:8] + "X" + clean[8:]
    if len(clean) == 12:
        return clean
    raise MtParseError(f"cannot build an LT address from {bic!r}")


def _bic_from_lt(lt: str) -> str:
    """The reverse: drop the LT identifier at position 9."""
    clean = (lt or "").strip().upper()
    if len(clean) != 12:
        return clean
    branch = clean[9:]
    return clean[:8] if branch == "XXX" else clean[:8] + branch


def iter_tags(message: MtMessage, tag: str) -> Iterator[str]:
    yield from message.get_all(tag)


def amount_field_32a(value_date: str, currency: str, amount: str) -> str:
    """:32A: is YYMMDD + currency + amount with a comma separator."""
    return f"{value_date}{currency}{amount}"


def split_32a(value: str) -> tuple[str, str, str]:
    """Split ``260928EUR1234,56`` into date, currency and amount."""
    if len(value) < 10:
        raise MtParseError(f"field 32A too short: {value!r}")
    return value[:6], value[6:9], value[9:]


def split_party(value: str) -> tuple[str, list[str]]:
    """Split a party field into its account line and its name/address lines.

    ``/GB33BUKB20201555555555\\nACME LTD\\n1 HIGH ST`` becomes the account and
    the two text lines.
    """
    lines = [line for line in value.split("\n") if line.strip()]
    account = ""
    if lines and lines[0].startswith("/"):
        account = lines[0].lstrip("/").strip()
        lines = lines[1:]
    return account, [line.strip() for line in lines]
