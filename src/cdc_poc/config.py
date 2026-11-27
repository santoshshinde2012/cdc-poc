"""Configuration objects. Read once in a composition root, then passed in; no module globals."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType

#: Primary key columns per captured table. The Debezium HTTP sink sends only the event
#: value (not the record key), so the key is rebuilt from the row image with these columns.
DEFAULT_TABLE_KEYS: Mapping[str, Sequence[str]] = MappingProxyType({"customers": ("id",), "orders": ("id",)})


@dataclass(frozen=True, slots=True)
class ReceiverConfig:
    bronze_path: Path
    host: str = "0.0.0.0"
    port: int = 8080
    table_keys: Mapping[str, Sequence[str]] = field(default_factory=lambda: DEFAULT_TABLE_KEYS)

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> ReceiverConfig:
        return cls(
            bronze_path=Path(env.get("BRONZE_PATH", "./data/bronze/events")),
            host=env.get("HOST", "0.0.0.0"),
            port=int(env.get("PORT", "8080")),
        )


@dataclass(frozen=True, slots=True)
class SilverConfig:
    data_dir: Path
    table_keys: Mapping[str, Sequence[str]] = field(default_factory=lambda: DEFAULT_TABLE_KEYS)
    source_schema: str = "public"
    topic_prefix: str = "cdc"

    @property
    def bronze_path(self) -> Path:
        return self.data_dir / "bronze" / "events"

    @property
    def silver_root(self) -> Path:
        return self.data_dir / "silver"

    @property
    def checkpoint_path(self) -> Path:
        return self.silver_root / "_checkpoint.json"

    @property
    def seen_keys_path(self) -> Path:
        return self.silver_root / "_seen_idempotency_keys"

    def destination(self, table: str) -> str:
        return f"{self.topic_prefix}.{self.source_schema}.{table}"

    @classmethod
    def from_env(cls, env: Mapping[str, str], default_root: Path) -> SilverConfig:
        return cls(data_dir=Path(env.get("CDC_DATA", str(default_root / "data"))))
