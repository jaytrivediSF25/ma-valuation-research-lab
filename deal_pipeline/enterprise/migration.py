from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict

from .models import CanonicalEnvelope, ModelVersion


@dataclass
class Migration:
    source: str
    target: str
    transform: Callable[[CanonicalEnvelope], CanonicalEnvelope]


class MigrationRegistry:
    def __init__(self) -> None:
        self._migrations: Dict[str, Migration] = {}

    def register(self, migration: Migration) -> None:
        key = f"{migration.source}->{migration.target}"
        self._migrations[key] = migration

    def migrate(self, env: CanonicalEnvelope, target: str) -> CanonicalEnvelope:
        current = env.version.as_semver()
        if current == target:
            return env
        key = f"{current}->{target}"
        if key not in self._migrations:
            raise ValueError(f"No migration path for {key}")
        out = self._migrations[key].transform(env)
        major, minor, patch = [int(x) for x in target.split(".")]
        out.version = ModelVersion(major=major, minor=minor, patch=patch)
        return out


def default_registry() -> MigrationRegistry:
    reg = MigrationRegistry()

    def _migrate_100_to_110(env: CanonicalEnvelope) -> CanonicalEnvelope:
        env.metadata.setdefault("migrations", []).append("1.0.0_to_1.1.0")
        env.metadata.setdefault("schema_note", "sub_sector_added")
        return env

    reg.register(Migration(source="1.0.0", target="1.1.0", transform=_migrate_100_to_110))
    return reg
