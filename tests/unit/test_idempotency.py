from __future__ import annotations

import hashlib

from cdc_poc.ingest.idempotency import Sha256IdempotencyKey


def test_same_inputs_give_same_key() -> None:
    k = Sha256IdempotencyKey()
    assert k.key("cdc.public.customers", '{"id":1}', "update", "p1") == k.key(
        "cdc.public.customers", '{"id":1}', "update", "p1"
    )


def test_every_part_changes_the_key() -> None:
    k = Sha256IdempotencyKey()
    base = k.key("d", "k", "update", "p")
    assert (
        len(
            {
                base,
                k.key("x", "k", "update", "p"),
                k.key("d", "x", "update", "p"),
                k.key("d", "k", "delete", "p"),
                k.key("d", "k", "update", "x"),
            }
        )
        == 5
    )


def test_formula_is_sha256_of_pipe_joined_parts() -> None:
    expected = hashlib.sha256(b"d||heartbeat|7").hexdigest()
    assert Sha256IdempotencyKey().key("d", None, "heartbeat", "7") == expected
