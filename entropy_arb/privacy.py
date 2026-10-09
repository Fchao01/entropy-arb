"""Bounded log capture with credential and authentication-value redaction."""
from __future__ import annotations

import re

MASK = b"[REDACTED]"
HEX_SECRET = re.compile(rb"\b(?:0x)?[0-9a-f]{64}\b|\b0x[0-9a-f]{40}\b", re.IGNORECASE)
AUTH_VALUE = re.compile(
    rb"(\b(?:authorization|auth|token|secret|signature|[a-z0-9_]*(?:private_key|signing_key|api_key))"
    rb"['\"]?\s*[:=]\s*)(?:\[REDACTED\]|\"[^\"]*\"|'[^']*'|[^\s,}\]]+)",
    re.IGNORECASE,
)
BEARER = re.compile(rb"\bBearer\s+[^\s,'\"}\]]+", re.IGNORECASE)


class PrivateLogCapture:
    def __init__(self, credentials, max_line_bytes=65536):
        values = set()
        for name, value in credentials.items():
            if value and any(fragment in name for fragment in ("PRIVATE_KEY", "SIGNING_KEY", "ACCOUNT_ADDRESS")):
                values.add(value.encode("utf-8"))
                if value.startswith("0x") and len(value) > 2:
                    values.add(value[2:].encode("utf-8"))
        self.literal = re.compile(b"|".join(re.escape(value) for value in sorted(values, key=len, reverse=True)),
                                  re.IGNORECASE) if values else None
        self.max_line_bytes = max_line_bytes
        self.pending = bytearray()
        self.discarding = False

    def redact(self, content):
        if self.literal is not None:
            content = self.literal.sub(MASK, content)
        content = HEX_SECRET.sub(MASK, content)
        content = BEARER.sub(b"Bearer " + MASK, content)
        return AUTH_VALUE.sub(lambda match: match.group(1) + MASK, content)

    def feed(self, chunk):
        output = bytearray()
        parts = chunk.split(b"\n")
        for index, part in enumerate(parts):
            if not self.discarding:
                self.pending.extend(part)
                if len(self.pending) > self.max_line_bytes:
                    self.pending.clear()
                    self.discarding = True
                    output.extend(b"[LOG LINE OMITTED: exceeded privacy capture limit]\n")
            if index < len(parts) - 1:
                if not self.discarding:
                    output.extend(self.redact(bytes(self.pending)) + b"\n")
                self.pending.clear()
                self.discarding = False
        return bytes(output)

    def finish(self):
        output = self.redact(bytes(self.pending)) if not self.discarding else b""
        self.pending.clear()
        self.discarding = False
        return output
