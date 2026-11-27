from __future__ import annotations

import json
import logging

import pytest

from cdc_poc.logging_setup import JsonFormatter, configure_logging


def record(msg: str, **extra: object) -> logging.LogRecord:
    rec = logging.LogRecord("receiver", logging.INFO, __file__, 1, msg, (), None)
    rec.__dict__.update(extra)
    return rec


def test_json_line_has_standard_and_extra_fields() -> None:
    line = JsonFormatter().format(record("committed to bronze", events=7))
    data = json.loads(line)
    assert data["msg"] == "committed to bronze"
    assert data["events"] == 7
    assert data["level"] == "INFO"
    assert data["logger"] == "receiver"
    assert data["ts"].endswith("+00:00")
    assert "\n" not in line


def test_exception_is_one_field() -> None:
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        rec = logging.LogRecord("x", logging.ERROR, __file__, 1, "failed", (), sys.exc_info())
    data = json.loads(JsonFormatter().format(rec))
    assert "ValueError: boom" in data["exc"]


@pytest.mark.parametrize(("fmt", "is_json"), [("json", True), ("", True), ("text", False)])
def test_configure_logging_formats(fmt: str, is_json: bool) -> None:
    root = logging.getLogger()
    saved = list(root.handlers), root.level
    try:
        configure_logging({"LOG_FORMAT": fmt} if fmt else {})
        assert isinstance(root.handlers[0].formatter, JsonFormatter) is is_json
    finally:
        root.handlers[:] = saved[0]
        root.setLevel(saved[1])
