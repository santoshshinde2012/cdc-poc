"""Composition root for ./run.sh verify: Postgres (source) against the Delta Silver tables."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from ..config import SilverConfig
from ..storage.delta import DeltaSilverStore
from ..storage.postgres import PsqlSourceReader, SourceUnavailableError
from .differ import SilverVerifier, TableDiff, VerifyReport

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def render(report: VerifyReport) -> str:
    lines = []
    for t in report.tables:
        mark = "OK      " if t.equal else "MISMATCH"
        lines.append(f"{mark} {t.table:<10} source {t.source_rows:>5} rows, silver {t.silver_rows:>5} rows")
        if not t.silver_built:
            lines.append("         Silver is empty or not built yet. Run ./run.sh silver")
        lines += _sample_lines(t)
    lines.append(
        "verify: Silver matches Postgres row by row" if report.ok else "verify: FAILED, Silver differs from Postgres"
    )
    return "\n".join(lines)


def _sample_lines(t: TableDiff) -> list[str]:
    cols = ", ".join(t.columns)
    out = []
    for label, rows in (("only in Postgres", t.only_in_source), ("only in Silver  ", t.only_in_silver)):
        if rows:
            out.append(f"         {label} ({cols}):")
            out += [f"           {', '.join(r)}" for r in rows]
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Row-level diff of Postgres against Silver. Exit 1 on any mismatch.")
    parser.add_argument("--json", action="store_true", help="print the report as JSON")
    args = parser.parse_args()
    config = SilverConfig.from_env(os.environ, PROJECT_ROOT)
    verifier = SilverVerifier(PsqlSourceReader(PROJECT_ROOT), DeltaSilverStore(config.silver_root))
    try:
        report = verifier.verify(list(config.table_keys))
    except SourceUnavailableError as exc:
        print(f"verify: {exc}", file=sys.stderr)
        sys.exit(2)
    print(json.dumps(report.to_dict(), indent=2) if args.json else render(report))
    sys.exit(0 if report.ok else 1)


if __name__ == "__main__":
    main()
