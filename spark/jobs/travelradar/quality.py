"""Data-quality checks for cleaned tables, driven by each source's `quality_rules`.

Rule keys per column:
    not_null: True            value must be present
    range: (lo, hi)           value within [lo, hi] when present
    regex: "..."              text matches when present
    allowed: [...]            value is one of the list when present
    max_lag_seconds: n        timestamp at most n seconds before `ingest_time` (staleness) and
                              not more than 60 s after it (clock skew / future timestamps)

All checks of a table run in one aggregation pass. `evaluate` returns one result per check with
the number of failing rows, so the same function feeds unit tests, the batch quality view and the
test report.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

FUTURE_TOLERANCE_SECONDS = 60


@dataclass
class CheckResult:
    table: str
    column: str
    check: str
    failed: int
    total: int

    @property
    def rate(self) -> float:
        return self.failed / self.total if self.total else 0.0

    def as_dict(self) -> dict:
        return {**asdict(self), "rate": round(self.rate, 6)}


def _violations(col: str, rules: dict) -> dict[str, Column]:
    c = F.col(col)
    present = c.isNotNull()
    out: dict[str, Column] = {}
    if rules.get("not_null"):
        out["not_null"] = c.isNull()
    if "range" in rules:
        lo, hi = rules["range"]
        out["range"] = present & ~c.between(lo, hi)
    if "regex" in rules:
        out["regex"] = present & ~c.rlike(rules["regex"])
    if "allowed" in rules:
        out["allowed"] = present & ~c.isin(list(rules["allowed"]))
    if "max_lag_seconds" in rules:
        lag = F.col("ingest_time").cast("double") - c.cast("double")
        out["staleness"] = present & (lag > rules["max_lag_seconds"])
        out["future"] = present & (lag < -FUTURE_TOLERANCE_SECONDS)
    return out


def evaluate(df: DataFrame, table: str, rules: dict,
             unique_keys: tuple[str, ...] = ()) -> list[CheckResult]:
    checks = [(col, name, cond) for col, r in rules.items() for name, cond in _violations(col, r).items()]
    aggs = [F.count(F.lit(1)).alias("__total")]
    aggs += [F.sum(F.when(cond, 1).otherwise(0)).alias(f"__{i}") for i, (_, _, cond) in enumerate(checks)]
    row = df.agg(*aggs).first()
    total = row["__total"]
    results = [CheckResult(table, col, name, int(row[f"__{i}"] or 0), total)
               for i, (col, name, _) in enumerate(checks)]
    if unique_keys:
        distinct = df.select(*unique_keys).distinct().count()
        results.append(CheckResult(table, ",".join(unique_keys), "unique", total - distinct, total))
    return results


def failing(results: list[CheckResult], max_rate: float = 0.0) -> list[CheckResult]:
    return [r for r in results if r.rate > max_rate]
