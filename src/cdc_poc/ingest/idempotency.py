"""Idempotency key: the same event always gets the same key, so a redelivery can be removed."""

from __future__ import annotations

import hashlib


class Sha256IdempotencyKey:
    """sha256(destination | key | operation | position), like the Zerobus envelope key."""

    def key(self, destination: str, key: str | None, operation: str, position: str) -> str:
        raw = "|".join([destination, key or "", operation, position])
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()
