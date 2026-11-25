"""Builds the test-case table (objective, steps, expected, actual, result) from the JUnit reports.

    python3 tests/make_report.py      # after ./run_tests.sh; writes tests/reports/test_cases.{md,csv}

Sources: spark/tests/reports/spark.xml, tests/reports/backend.xml, tests/reports/repo.xml and
tests/reports/integration.xml. Each test's docstring is "<ID>: <title>" followed by Objective:,
Steps: and Expected: lines; the observed result is the `actual` property the test recorded.
"""
from __future__ import annotations

import csv
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
REPORTS = {
    "Spark (unit, in the cluster image)": REPO / "spark/tests/reports/spark.xml",
    "Backend API": REPO / "tests/reports/backend.xml",
    "NiFi flow definitions": REPO / "tests/reports/repo.xml",
    "Integration (running stack)": REPO / "tests/reports/integration.xml",
}
OUT = REPO / "tests/reports"
FIELDS = ["suite", "id", "title", "objective", "steps", "expected", "actual", "result", "seconds"]


def parse_doc(doc: str) -> dict:
    out = {"id": "", "title": "", "objective": "", "steps": "", "expected": ""}
    lines = doc.strip().splitlines()
    if lines:
        m = re.match(r"([A-Z]+-[A-Z]+-\d+|[A-Z]+-\d+):\s*(.*)", lines[0])
        if m:
            out["id"], out["title"] = m.groups()
    key = None
    for line in lines[1:]:
        m = re.match(r"(Objective|Steps|Expected):\s*(.*)", line)
        if m:
            key = m.group(1).lower()
            out[key] = m.group(2)
        elif key:
            out[key] += " " + line.strip()
    return out


def cases():
    for suite, path in REPORTS.items():
        if not path.exists():
            continue
        for tc in ET.parse(path).getroot().iter("testcase"):
            props = {p.get("name"): p.get("value") for p in tc.iter("property")}
            skipped = tc.find("skipped")
            if skipped is not None and "pytest -m integration" in (skipped.get("message") or ""):
                continue   # integration test in a static run: reported by the integration suite
            row = parse_doc(props.get("doc", ""))
            param = re.search(r"\[(.+)\]$", tc.get("name", ""))
            if param:
                row["id"] += f" [{param.group(1)}]"
            if tc.find("failure") is not None or tc.find("error") is not None:
                result = "FAIL"
                node = tc.find("failure") if tc.find("failure") is not None else tc.find("error")
                actual = (node.get("message") or "")[:300]
            elif tc.find("skipped") is not None:
                result = "SKIPPED"
                actual = tc.find("skipped").get("message", "")
            else:
                result = "PASS"
                actual = props.get("actual", "")
            yield {"suite": suite, **row, "actual": actual, "result": result,
                   "seconds": f"{float(tc.get('time', 0)):.2f}", "name": tc.get("name")}


def md_escape(text: str) -> str:
    return (text or "").replace("|", "\\|").replace("\n", " ")


def main() -> None:
    rows = sorted(cases(), key=lambda r: (list(REPORTS).index(r["suite"]), r["id"]))
    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / "test_cases.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    lines = [f"# TravelRadar test cases", "",
             f"Generated {datetime.now():%Y-%m-%d %H:%M} by tests/make_report.py.", ""]
    lines += ["| Suite | Tests | Passed | Failed | Skipped |", "|---|---|---|---|---|"]
    for suite in REPORTS:
        rs = [r for r in rows if r["suite"] == suite]
        if rs:
            count = lambda s: sum(r["result"] == s for r in rs)
            lines.append(f"| {suite} | {len(rs)} | {count('PASS')} | {count('FAIL')} | {count('SKIPPED')} |")
    for suite in REPORTS:
        rs = [r for r in rows if r["suite"] == suite]
        if not rs:
            continue
        lines += ["", f"## {suite}", "",
                  "| ID | Objective | Steps | Expected result | Actual result | Result |",
                  "|---|---|---|---|---|---|"]
        for r in rs:
            lines.append("| " + " | ".join(md_escape(x) for x in [
                f"**{r['id']}** {r['title']}", r["objective"], r["steps"], r["expected"],
                r["actual"], r["result"]]) + " |")
    (OUT / "test_cases.md").write_text("\n".join(lines) + "\n")
    missing = [r["name"] for r in rows if not r["id"]]
    print(f"{len(rows)} test cases -> {OUT / 'test_cases.md'} and test_cases.csv")
    if missing:
        print("tests without an ID in their docstring:", missing)


if __name__ == "__main__":
    main()
