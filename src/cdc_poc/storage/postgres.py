"""SourceReader over Postgres, using psql inside the compose service (no extra Python driver)."""

from __future__ import annotations

import json
import subprocess
from decimal import Decimal
from pathlib import Path
from typing import Any


class SourceUnavailableError(RuntimeError):
    """Postgres could not be queried (stack down, container missing, bad table)."""


class PsqlSourceReader:
    def __init__(self, project_dir: Path, schema: str = "public", service: str = "postgres") -> None:
        self._dir = project_dir
        self._schema = schema
        self._service = service

    def rows(self, table: str) -> list[dict[str, Any]]:
        sql = f"SELECT coalesce(json_agg(t ORDER BY id), '[]') FROM {self._schema}.{table} t"
        cmd = ["docker", "compose", "exec", "-T", "-e", "PGTZ=UTC", self._service]
        cmd += ["psql", "-U", "app", "-d", "shop", "-v", "ON_ERROR_STOP=1", "-At", "-c", sql]
        try:
            done = subprocess.run(cmd, cwd=self._dir, capture_output=True, text=True, timeout=60, check=False)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise SourceUnavailableError(f"cannot run docker compose: {exc}") from exc
        if done.returncode != 0:
            detail = (done.stderr or done.stdout).strip().splitlines()[-1:] or ["no output"]
            raise SourceUnavailableError(
                f"cannot read {self._schema}.{table} from Postgres ({detail[0]}). Is the stack up? Run ./run.sh up"
            )
        rows = json.loads(done.stdout.strip() or "[]", parse_float=Decimal)
        return list(rows)
