"""Quality rule specs: load and validate YAML rule files.

Pure Python (only needs PyYAML), so CI can lint every rule file
without starting Spark (see lint.py).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

ERROR, WARN = "error", "warn"
ROW, DATASET = "row", "dataset"

# rule name -> (level, required params, needs columns?)
RULE_CATALOG: dict[str, tuple[str, tuple[str, ...], bool]] = {
    "not_null":  (ROW,     (),          True),
    "in_set":    (ROW,     ("values",), True),
    "between":   (ROW,     (),          True),   # min and/or max
    "regex":     (ROW,     ("pattern",), True),
    "expr":      (ROW,     ("expr",),   False),  # SQL boolean that must be TRUE
    "unique":    (DATASET, (),          True),
    "row_count": (DATASET, (),          False),  # min and/or max
}


class SpecError(ValueError):
    pass


@dataclass(frozen=True)
class Check:
    name: str
    rule: str
    columns: tuple[str, ...] = ()
    severity: str = ERROR
    params: dict[str, Any] = field(default_factory=dict)

    @property
    def level(self) -> str:
        return RULE_CATALOG[self.rule][0]


@dataclass(frozen=True)
class TableSpec:
    table: str
    checks: tuple[Check, ...]
    fail_if_quarantine_ratio_above: float = 0.05

    @property
    def row_checks(self) -> list[Check]:
        return [c for c in self.checks if c.level == ROW]

    @property
    def dataset_checks(self) -> list[Check]:
        return [c for c in self.checks if c.level == DATASET]


def _parse_check(raw: dict, where: str) -> Check:
    if not isinstance(raw, dict):
        raise SpecError(f"{where}: each check must be a mapping")
    unknown = set(raw) - {"name", "rule", "columns", "severity", "params"}
    if unknown:
        raise SpecError(f"{where}: unknown keys {sorted(unknown)}")
    name, rule = raw.get("name"), raw.get("rule")
    if not name or not rule:
        raise SpecError(f"{where}: 'name' and 'rule' are required")
    if rule not in RULE_CATALOG:
        raise SpecError(f"{where} ({name}): unknown rule '{rule}'. "
                        f"Known: {sorted(RULE_CATALOG)}")
    level, required, needs_cols = RULE_CATALOG[rule]
    cols = raw.get("columns") or []
    if isinstance(cols, str):
        cols = [cols]
    if needs_cols and not cols:
        raise SpecError(f"{where} ({name}): rule '{rule}' needs 'columns'")
    params = raw.get("params") or {}
    missing = [p for p in required if p not in params]
    if missing:
        raise SpecError(f"{where} ({name}): missing params {missing}")
    if rule in ("between", "row_count") and not ({"min", "max"} & set(params)):
        raise SpecError(f"{where} ({name}): '{rule}' needs 'min' and/or 'max'")
    severity = raw.get("severity", ERROR)
    if severity not in (ERROR, WARN):
        raise SpecError(f"{where} ({name}): severity must be 'error' or 'warn'")
    return Check(name=name, rule=rule, columns=tuple(cols),
                 severity=severity, params=params)


def parse(doc: dict, source: str = "<dict>") -> TableSpec:
    if not isinstance(doc, dict) or "table" not in doc:
        raise SpecError(f"{source}: top level needs 'table'")
    raw_checks = doc.get("checks") or []
    checks = tuple(_parse_check(c, f"{source} checks[{i}]")
                   for i, c in enumerate(raw_checks))
    names = [c.name for c in checks]
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        raise SpecError(f"{source}: duplicate check names {dupes}")
    ratio = float(doc.get("fail_if_quarantine_ratio_above", 0.05))
    if not 0 <= ratio <= 1:
        raise SpecError(f"{source}: fail_if_quarantine_ratio_above must be 0..1")
    return TableSpec(table=doc["table"], checks=checks,
                     fail_if_quarantine_ratio_above=ratio)


def load(path: str | Path) -> TableSpec:
    path = Path(path)
    return parse(yaml.safe_load(path.read_text()), str(path))
