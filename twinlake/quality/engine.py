"""Quality engine: apply a TableSpec to a DataFrame.

Spark Connect safe: only DataFrame/Column APIs (no RDDs, no sparkContext,
no cache), so the same code runs on Sail locally and Databricks serverless.

Usage:
    spec = load("conf/quality/silver_sales.yml")
    result = run_checks(df, spec)
    result.raise_if_failed()          # stop the job on hard failures
    write(result.valid, "silver.sales")
    write(result.quarantine, "quarantine.silver_sales", mode="append")
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from typing import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .spec import DATASET, ERROR, RULE_CATALOG, ROW, WARN, Check, TableSpec

ERR_COL, WARN_COL, TS_COL = "_dq_errors", "_dq_warnings", "_dq_checked_at"

RowRule = Callable[[Check], Column]                      # TRUE = row fails
DatasetRule = Callable[[DataFrame, Check], tuple[bool, str]]  # (failed, detail)

_ROW_RULES: dict[str, RowRule] = {}
_DATASET_RULES: dict[str, DatasetRule] = {}


def register_rule(name: str, level: str, required: tuple[str, ...] = (),
                  needs_columns: bool = True):
    """Add a team rule once; it becomes usable from YAML everywhere."""
    def deco(fn):
        RULE_CATALOG[name] = (level, required, needs_columns)
        (_ROW_RULES if level == ROW else _DATASET_RULES)[name] = fn
        return fn
    return deco


def _any(cols: list[Column]) -> Column:
    return reduce(lambda a, b: a | b, cols)


# ---- built-in row rules (NULL input counts as pass; use not_null for that)
@register_rule("not_null", ROW)
def _not_null(c: Check) -> Column:
    return _any([F.col(x).isNull() for x in c.columns])


@register_rule("in_set", ROW, ("values",))
def _in_set(c: Check) -> Column:
    v = c.params["values"]
    return _any([F.col(x).isNotNull() & ~F.col(x).isin(v) for x in c.columns])


@register_rule("between", ROW)
def _between(c: Check) -> Column:
    lo, hi = c.params.get("min"), c.params.get("max")
    parts = []
    for x in c.columns:
        if lo is not None:
            parts.append(F.col(x) < F.lit(lo))
        if hi is not None:
            parts.append(F.col(x) > F.lit(hi))
    return _any(parts)


@register_rule("regex", ROW, ("pattern",))
def _regex(c: Check) -> Column:
    p = c.params["pattern"]
    return _any([F.col(x).isNotNull() & ~F.col(x).rlike(p) for x in c.columns])


@register_rule("expr", ROW, ("expr",), needs_columns=False)
def _expr(c: Check) -> Column:
    return ~F.expr(c.params["expr"])


# ---- built-in dataset rules
@register_rule("unique", DATASET)
def _unique(df: DataFrame, c: Check) -> tuple[bool, str]:
    dupes = (df.groupBy(*c.columns).count()
               .filter(F.col("count") > 1).count())
    return dupes > 0, f"{dupes} duplicate key(s) on {list(c.columns)}"


@register_rule("row_count", DATASET, needs_columns=False)
def _row_count(df: DataFrame, c: Check) -> tuple[bool, str]:
    n = df.count()
    lo, hi = c.params.get("min"), c.params.get("max")
    failed = (lo is not None and n < lo) or (hi is not None and n > hi)
    return failed, f"{n} rows (min={lo}, max={hi})"


# ---- engine
class DataQualityError(RuntimeError):
    pass


@dataclass
class DQResult:
    table: str
    valid: DataFrame         # rows with no error-level failures, DQ cols dropped
    quarantine: DataFrame    # rows with error-level failures + reasons
    summary: list[dict]      # one entry per check
    total_rows: int
    quarantined_rows: int
    max_ratio: float

    @property
    def quarantine_ratio(self) -> float:
        return self.quarantined_rows / self.total_rows if self.total_rows else 0.0

    @property
    def failed(self) -> list[str]:
        reasons = [f"{s['check']}: {s['detail']}" for s in self.summary
                   if s["level"] == DATASET and s["severity"] == ERROR
                   and s["failed"]]
        if self.quarantine_ratio > self.max_ratio:
            reasons.append(f"quarantine ratio {self.quarantine_ratio:.2%} "
                           f"> allowed {self.max_ratio:.2%}")
        return reasons

    def raise_if_failed(self) -> None:
        if self.failed:
            raise DataQualityError(f"{self.table}: " + "; ".join(self.failed))

    def report(self) -> str:
        lines = [f"DQ {self.table}: {self.total_rows} rows, "
                 f"{self.quarantined_rows} quarantined"]
        for s in self.summary:
            mark = "FAIL" if s["failed"] else "ok  "
            lines.append(f"  {mark} [{s['severity']}] {s['check']}: {s['detail']}")
        return "\n".join(lines)


def _reasons(checks: list[Check]) -> Column:
    if not checks:
        return F.lit("")
    # concat_ws skips NULLs, so passing rules simply disappear
    return F.concat_ws(",", *[
        F.when(F.coalesce(_ROW_RULES[c.rule](c), F.lit(False)), F.lit(c.name))
        for c in checks])


def run_checks(df: DataFrame, spec: TableSpec) -> DQResult:
    row_checks = spec.row_checks
    err_checks = [c for c in row_checks if c.severity == ERROR]
    warn_checks = [c for c in row_checks if c.severity == WARN]

    # one aggregation pass for all row-rule counts + total
    aggs = [F.count(F.lit(1)).alias("__total")] + [
        F.sum(F.when(F.coalesce(_ROW_RULES[c.rule](c), F.lit(False)), 1)
               .otherwise(0)).alias(f"__c{i}")
        for i, c in enumerate(row_checks)]
    if err_checks:
        bad_row = _any([F.coalesce(_ROW_RULES[c.rule](c), F.lit(False))
                        for c in err_checks])
        aggs.append(F.sum(F.when(bad_row, 1).otherwise(0)).alias("__bad"))
    stats = df.agg(*aggs).collect()[0]
    total = int(stats["__total"])
    bad = int(stats["__bad"] or 0) if err_checks else 0

    summary = []
    for i, c in enumerate(row_checks):
        n = int(stats[f"__c{i}"] or 0)
        summary.append({"check": c.name, "rule": c.rule, "level": ROW,
                        "severity": c.severity, "failed": n > 0,
                        "failed_rows": n, "detail": f"{n} row(s) failed"})
    for c in spec.dataset_checks:
        failed, detail = _DATASET_RULES[c.rule](df, c)
        summary.append({"check": c.name, "rule": c.rule, "level": DATASET,
                        "severity": c.severity, "failed": failed,
                        "failed_rows": None, "detail": detail})

    flagged = (df.withColumn(ERR_COL, _reasons(err_checks))
                 .withColumn(WARN_COL, _reasons(warn_checks)))
    valid = flagged.filter(F.col(ERR_COL) == "").drop(ERR_COL, WARN_COL)
    quarantine = (flagged.filter(F.col(ERR_COL) != "")
                         .withColumn(TS_COL, F.current_timestamp()))

    return DQResult(table=spec.table, valid=valid, quarantine=quarantine,
                    summary=summary, total_rows=total, quarantined_rows=bad,
                    max_ratio=spec.fail_if_quarantine_ratio_above)
