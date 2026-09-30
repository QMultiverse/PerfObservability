"""ISO 20022 (MX) parsing and building.

Covers the AppHdr (head.001) plus the Document payloads in scope: pacs.008,
pacs.009 including COV, pacs.002, pacs.004 and camt.056 / camt.029.

Validation is in two layers:

* **structure** — the elements the Hub routes and settles on must be present
  and well-formed. Always on, implemented here.
* **XSD** — the official ISO 20022 / CBPR+ schemas. Those come from Swift
  MyStandards and are not redistributable, so :class:`XsdValidator` loads them
  from a directory if one is configured and is otherwise a no-op. See
  :data:`MX_SCHEMA_DIR_ENV`.

``lxml`` is used with entity resolution and network access off: an inbound
payment is untrusted input.
"""

from __future__ import annotations

import datetime as dt
import os
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Any, Final

from lxml import etree

MX_SCHEMA_DIR_ENV: Final = "HUB_MX_SCHEMA_DIR"

APPHDR_NS: Final = "urn:iso:std:iso:20022:tech:xsd:head.001.001.02"
ENVELOPE_NS: Final = "urn:swift:xsd:envelope"

# Namespace prefix to full URN for the message types in scope.
DOCUMENT_NS: Final = {
    "pacs.008.001.08": "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08",
    "pacs.009.001.08": "urn:iso:std:iso:20022:tech:xsd:pacs.009.001.08",
    "pacs.002.001.10": "urn:iso:std:iso:20022:tech:xsd:pacs.002.001.10",
    "pacs.004.001.09": "urn:iso:std:iso:20022:tech:xsd:pacs.004.001.09",
    "camt.056.001.08": "urn:iso:std:iso:20022:tech:xsd:camt.056.001.08",
    "camt.029.001.09": "urn:iso:std:iso:20022:tech:xsd:camt.029.001.09",
}

_NS_RE: Final = re.compile(
    r"^urn:iso:std:iso:20022:tech:xsd:(?P<name>[a-z]{4}\.\d{3}\.\d{3}\.\d{2})$"
)

_PARSER: Final = etree.XMLParser(
    resolve_entities=False,
    no_network=True,
    huge_tree=False,
    remove_comments=True,
    remove_pis=True,
)


class MxParseError(ValueError):
    """The XML is malformed, or is not a message type the Hub handles."""


class MxValidationError(ValueError):
    """The XML parses but breaks a structural or schema rule."""

    def __init__(self, message: str, *, errors: Sequence[str] = ()) -> None:
        super().__init__(message)
        self.errors = list(errors)


# ---------------------------------------------------------------- parsing
def parse_xml(raw: str | bytes) -> etree._Element:
    """Parse untrusted XML with entities and network access disabled."""
    data = raw.encode("utf-8") if isinstance(raw, str) else raw
    if not data.strip():
        raise MxParseError("empty XML payload")
    try:
        return etree.fromstring(data, parser=_PARSER)
    except etree.XMLSyntaxError as exc:
        raise MxParseError(f"malformed XML: {exc}") from exc


def local_name(element: etree._Element) -> str:
    tag = element.tag
    if isinstance(tag, str) and tag.startswith("{"):
        return tag.split("}", 1)[1]
    return str(tag)


def namespace_of(element: etree._Element) -> str:
    tag = element.tag
    if isinstance(tag, str) and tag.startswith("{"):
        return tag[1:].split("}", 1)[0]
    return ""


def msg_type_of(document: etree._Element) -> str:
    """The ISO name from the Document namespace, e.g. ``pacs.008.001.08``."""
    match = _NS_RE.match(namespace_of(document))
    if match is None:
        raise MxParseError(f"unrecognised Document namespace {namespace_of(document)!r}")
    return match.group("name")


@dataclass(slots=True)
class AppHdr:
    """head.001 business application header."""

    from_bic: str = ""
    to_bic: str = ""
    biz_msg_id: str = ""
    msg_def_id: str = ""
    biz_svc: str = ""
    creation_dt: str = ""
    copy_duplicate: str = ""
    possible_duplicate: bool = False
    related: str = ""


def parse_app_hdr(raw: str | bytes) -> AppHdr:
    root = parse_xml(raw)
    if local_name(root) != "AppHdr":
        raise MxParseError(f"expected AppHdr, got {local_name(root)!r}")
    get = _finder(root)
    return AppHdr(
        from_bic=get("Fr/FIId/FinInstnId/BICFI") or get("Fr/OrgId/Id/OrgId/AnyBIC"),
        to_bic=get("To/FIId/FinInstnId/BICFI") or get("To/OrgId/Id/OrgId/AnyBIC"),
        biz_msg_id=get("BizMsgIdr"),
        msg_def_id=get("MsgDefIdr"),
        biz_svc=get("BizSvc"),
        creation_dt=get("CreDt"),
        copy_duplicate=get("CpyDplct"),
        possible_duplicate=get("PssblDplct").lower() in {"true", "1"},
        related=get("Rltd/BizMsgIdr"),
    )


def _finder(root: etree._Element) -> Any:
    """Namespace-agnostic path lookup.

    ISO 20022 documents are single-namespace, so matching on local names keeps
    the parser working across the minor versions a counterparty may send.
    """

    def get(path: str, node: etree._Element | None = None) -> str:
        element = find(node if node is not None else root, path)
        return (element.text or "").strip() if element is not None else ""

    return get


def find(node: etree._Element, path: str) -> etree._Element | None:
    current: etree._Element | None = node
    for step in path.split("/"):
        if current is None:
            return None
        current = next((c for c in current if local_name(c) == step), None)
    return current


def find_all(node: etree._Element, path: str) -> list[etree._Element]:
    """All nodes matching ``path``; the last step may repeat."""
    *head, last = path.split("/")
    current: etree._Element | None = node
    for step in head:
        if current is None:
            return []
        current = next((c for c in current if local_name(c) == step), None)
    if current is None:
        return []
    return [c for c in current if local_name(c) == last]


def text_of(node: etree._Element | None, path: str = "") -> str:
    if node is None:
        return ""
    target = find(node, path) if path else node
    return (target.text or "").strip() if target is not None else ""


@dataclass(slots=True)
class MxMessage:
    """A parsed MX: the header, the document root and its type."""

    msg_type: str
    document: etree._Element
    app_hdr: AppHdr = field(default_factory=AppHdr)
    raw_document: bytes = b""
    raw_app_hdr: bytes = b""

    @property
    def body(self) -> etree._Element:
        """The single child of Document — ``FIToFICstmrCdtTrf`` and friends."""
        children = list(self.document)
        if not children:
            raise MxParseError(f"{self.msg_type} Document has no body")
        return children[0]

    def group_header(self) -> etree._Element | None:
        return find(self.body, "GrpHdr")

    def transactions(self) -> list[etree._Element]:
        """The transaction elements, whatever this message type calls them."""
        for name in ("CdtTrfTxInf", "TxInfAndSts", "TxInf", "UndrlygTxInf"):
            found = find_all(self.body, name)
            if found:
                return found
        return []

    def is_cover(self) -> bool:
        """pacs.009 COV carries the underlying customer transfer in ``UndrlygCstmrCdtTrf``."""
        return any(find(tx, "UndrlygCstmrCdtTrf") is not None for tx in self.transactions())


def parse_document(raw: str | bytes, app_hdr: str | bytes | None = None) -> MxMessage:
    """Parse a Document (and optionally its AppHdr) into :class:`MxMessage`."""
    document = parse_xml(raw)
    if local_name(document) == "Envelope":
        document = _unwrap_envelope(document)
    if local_name(document) != "Document":
        raise MxParseError(f"expected Document, got {local_name(document)!r}")
    msg_type = msg_type_of(document)
    header = parse_app_hdr(app_hdr) if app_hdr else AppHdr()
    return MxMessage(
        msg_type=msg_type,
        document=document,
        app_hdr=header,
        raw_document=raw.encode("utf-8") if isinstance(raw, str) else raw,
        raw_app_hdr=(app_hdr.encode("utf-8") if isinstance(app_hdr, str) else (app_hdr or b"")),
    )


def _unwrap_envelope(envelope: etree._Element) -> etree._Element:
    document = next((c for c in envelope if local_name(c) == "Document"), None)
    if document is None:
        raise MxParseError("Envelope contains no Document")
    return document


def peek_uetr(raw: str | bytes) -> str:
    """Pull the UETR out with a regex, without building a tree.

    The gRPC edge does light checks only. Parsing a multi-kilobyte pacs.008
    just to key the Kafka write would break the "no parsing on the edge path"
    rule.
    """
    text = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
    match = re.search(r"<(?:\w+:)?UETR>\s*([0-9a-fA-F-]{36})\s*</", text)
    return match.group(1).lower() if match else ""


def peek_msg_type(raw: str | bytes) -> str:
    """Read the message type from the Document namespace, without a tree."""
    text = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
    match = re.search(r"urn:iso:std:iso:20022:tech:xsd:([a-z]{4}\.\d{3}\.\d{3}\.\d{2})", text)
    return match.group(1) if match else ""


def peek_biz_msg_id(raw: str | bytes) -> str:
    text = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
    match = re.search(r"<(?:\w+:)?BizMsgIdr>\s*([^<]{1,40})\s*</", text)
    return match.group(1).strip() if match else ""


# ------------------------------------------------------------- validation
#: Elements the Hub cannot route or settle without, per message type.
REQUIRED_PATHS: Final[dict[str, tuple[str, ...]]] = {
    "pacs.008.001.08": (
        "GrpHdr/MsgId",
        "GrpHdr/CreDtTm",
        "GrpHdr/NbOfTxs",
        "CdtTrfTxInf/PmtId/EndToEndId",
        "CdtTrfTxInf/PmtId/UETR",
        "CdtTrfTxInf/IntrBkSttlmAmt",
        "CdtTrfTxInf/Dbtr",
        "CdtTrfTxInf/Cdtr",
    ),
    "pacs.009.001.08": (
        "GrpHdr/MsgId",
        "GrpHdr/CreDtTm",
        "CdtTrfTxInf/PmtId/UETR",
        "CdtTrfTxInf/IntrBkSttlmAmt",
        "CdtTrfTxInf/Dbtr",
        "CdtTrfTxInf/Cdtr",
    ),
    "pacs.002.001.10": ("GrpHdr/MsgId", "TxInfAndSts/TxSts"),
    "pacs.004.001.09": ("GrpHdr/MsgId", "TxInf/RtrdIntrBkSttlmAmt"),
    "camt.056.001.08": ("Assgnmt/Id", "Undrlyg"),
    "camt.029.001.09": ("Assgnmt/Id", "Sts"),
}


def validate_structure(message: MxMessage) -> None:
    """Check the elements the Hub depends on. Raises on the first batch of errors."""
    required = REQUIRED_PATHS.get(message.msg_type)
    if required is None:
        raise MxValidationError(f"unsupported message type {message.msg_type}")

    try:
        body = message.body
    except MxParseError as exc:
        # An empty <Document/> is a structural failure, not a parse surprise:
        # report it the way every other missing element is reported.
        raise MxValidationError(
            f"{message.msg_type} failed structural validation", errors=[str(exc)]
        ) from exc

    errors: list[str] = []
    for path in required:
        head, _, tail = path.partition("/")
        if head in {"CdtTrfTxInf", "TxInfAndSts", "TxInf"}:
            transactions = find_all(body, head)
            if not transactions:
                errors.append(f"missing {head}")
                continue
            for index, tx in enumerate(transactions):
                if tail and find(tx, tail) is None:
                    errors.append(f"missing {head}[{index}]/{tail}")
        elif find(body, path) is None:
            errors.append(f"missing {path}")

    errors.extend(_amount_errors(message))
    errors.extend(_count_errors(message))
    if errors:
        raise MxValidationError(f"{message.msg_type} failed structural validation", errors=errors)


def _amount_errors(message: MxMessage) -> list[str]:
    errors: list[str] = []
    for index, tx in enumerate(message.transactions()):
        for name in ("IntrBkSttlmAmt", "RtrdIntrBkSttlmAmt", "InstdAmt"):
            element = find(tx, name)
            if element is None:
                continue
            currency = element.get("Ccy", "")
            if not re.fullmatch(r"[A-Z]{3}", currency):
                errors.append(f"tx[{index}]/{name} has currency {currency!r}")
            try:
                value = Decimal((element.text or "").strip())
            except Exception:
                errors.append(f"tx[{index}]/{name} is not a decimal")
                continue
            if value <= 0:
                errors.append(f"tx[{index}]/{name} must be positive")
    return errors


def _count_errors(message: MxMessage) -> list[str]:
    """NbOfTxs must match the transactions actually present."""
    header = message.group_header()
    if header is None:
        return []
    declared = text_of(header, "NbOfTxs")
    if not declared:
        return []
    try:
        expected = int(declared)
    except ValueError:
        return [f"GrpHdr/NbOfTxs is not a number: {declared!r}"]
    actual = len(message.transactions())
    return (
        []
        if expected == actual
        else [f"GrpHdr/NbOfTxs says {expected} but {actual} transactions are present"]
    )


class XsdValidator:
    """Optional XSD validation against locally supplied schemas.

    CBPR+ schemas are licensed through Swift MyStandards and are not shipped
    with this repository. Point :data:`MX_SCHEMA_DIR_ENV` at a directory of
    ``<msg_type>.xsd`` files to turn this on; with no directory it reports
    ``available == False`` and validates nothing.
    """

    def __init__(self, schema_dir: str | Path | None = None) -> None:
        raw = schema_dir if schema_dir is not None else os.environ.get(MX_SCHEMA_DIR_ENV, "")
        self.schema_dir = Path(raw) if raw else None

    @property
    def available(self) -> bool:
        return self.schema_dir is not None and self.schema_dir.is_dir()

    def schema_path(self, msg_type: str) -> Path | None:
        if self.schema_dir is None:
            return None
        candidate = self.schema_dir / f"{msg_type}.xsd"
        return candidate if candidate.is_file() else None

    def validate(self, message: MxMessage) -> None:
        path = self.schema_path(message.msg_type)
        if path is None:
            return
        schema = _load_schema(str(path))
        if not schema.validate(message.document):
            raise MxValidationError(
                f"{message.msg_type} failed XSD validation",
                errors=[str(e) for e in schema.error_log],
            )


@lru_cache(maxsize=32)
def _load_schema(path: str) -> etree.XMLSchema:
    return etree.XMLSchema(etree.parse(path, parser=_PARSER))


# --------------------------------------------------------------- building
def element(tag: str, text: str = "", **attrs: str) -> etree._Element:
    node = etree.Element(tag, {k: v for k, v in attrs.items() if v})
    if text:
        node.text = text
    return node


def serialise(node: etree._Element, *, declaration: bool = True) -> bytes:
    out: bytes = etree.tostring(
        node, xml_declaration=declaration, encoding="UTF-8", pretty_print=True
    )
    return out


def build_app_hdr(
    *,
    from_bic: str,
    to_bic: str,
    biz_msg_id: str,
    msg_def_id: str,
    creation_dt: str,
    biz_svc: str = "swift.cbprplus.02",
    possible_duplicate: bool = False,
) -> bytes:
    """Build a head.001 AppHdr."""
    ns = {None: APPHDR_NS}
    root = etree.Element(f"{{{APPHDR_NS}}}AppHdr", nsmap=ns)  # type: ignore[arg-type]
    fr = etree.SubElement(root, f"{{{APPHDR_NS}}}Fr")
    _bic_party(fr, from_bic)
    to = etree.SubElement(root, f"{{{APPHDR_NS}}}To")
    _bic_party(to, to_bic)
    etree.SubElement(root, f"{{{APPHDR_NS}}}BizMsgIdr").text = biz_msg_id
    etree.SubElement(root, f"{{{APPHDR_NS}}}MsgDefIdr").text = msg_def_id
    etree.SubElement(root, f"{{{APPHDR_NS}}}BizSvc").text = biz_svc
    etree.SubElement(root, f"{{{APPHDR_NS}}}CreDt").text = creation_dt
    if possible_duplicate:
        etree.SubElement(root, f"{{{APPHDR_NS}}}PssblDplct").text = "true"
    return serialise(root)


def _bic_party(parent: etree._Element, bic: str) -> None:
    fiid = etree.SubElement(parent, f"{{{APPHDR_NS}}}FIId")
    fin = etree.SubElement(fiid, f"{{{APPHDR_NS}}}FinInstnId")
    etree.SubElement(fin, f"{{{APPHDR_NS}}}BICFI").text = bic


class DocumentBuilder:
    """Small helper for assembling a Document in one namespace."""

    def __init__(self, msg_type: str) -> None:
        if msg_type not in DOCUMENT_NS:
            raise MxParseError(f"no namespace known for {msg_type}")
        self.msg_type = msg_type
        self.ns = DOCUMENT_NS[msg_type]
        self.root = etree.Element(f"{{{self.ns}}}Document", nsmap={None: self.ns})  # type: ignore[arg-type]

    def child(
        self, parent: etree._Element, tag: str, text: str = "", **attrs: str
    ) -> etree._Element:
        node = etree.SubElement(
            parent, f"{{{self.ns}}}{tag}", {k: v for k, v in attrs.items() if v}
        )
        if text:
            node.text = text
        return node

    def path(self, parent: etree._Element, path: str) -> etree._Element:
        """Create (or reuse) a chain of elements: ``PmtId/EndToEndId``."""
        node = parent
        for step in path.split("/"):
            existing = next((c for c in node if local_name(c) == step), None)
            node = existing if existing is not None else self.child(node, step)
        return node

    def to_bytes(self) -> bytes:
        return serialise(self.root)


def iso_datetime(moment: dt.datetime) -> str:
    """ISO 20022 CreDtTm: second precision, explicit UTC."""
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def all_text(node: etree._Element, paths: Iterable[str]) -> dict[str, str]:
    return {path: text_of(node, path) for path in paths}
