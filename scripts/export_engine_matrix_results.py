#!/usr/bin/env python3
"""Export combined engine matrix benchmark results to Markdown and Parquet."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

import build_dashboard  # noqa: E402

DEFAULT_RUN_IDS = (
    "engine-matrix-20260626T2335Z",
    "engine-matrix-medium-20260626T233151Z",
    "engine-matrix-large-20260626T233637Z",
)

PHASE_COLUMNS = (
    "startup",
    "duckdb_context",
    "load_config",
    "attach",
    "create_namespace",
    "load_catalog",
    "start_spark",
    "drop_existing_table",
    "create_table",
    "load_table",
    "insert",
    "append",
    "readback",
    "scan",
    "delete",
    "read_after_delete",
    "cleanup",
)


def json_text(value: Any) -> str:
    return json.dumps(value or {}, sort_keys=True, separators=(",", ":"))


def operation_seconds(row: dict[str, Any], phase_timings: dict[str, float]) -> float:
    engine = row.get("engine", "duckdb")
    if engine == "duckdb":
        prefixes = ("create_table", "insert", "readback", "delete", "read_after_delete")
        return sum(
            float(duration)
            for phase, duration in row.get("timings", {}).items()
            if any(str(phase).startswith(prefix) for prefix in prefixes)
        )
    return sum(
        float(duration)
        for phase, duration in phase_timings.items()
        if phase not in {"load_catalog", "start_spark"}
    )


def read_seconds(row: dict[str, Any], timings: dict[str, Any]) -> float:
    if row.get("engine", "duckdb") == "duckdb":
        return build_dashboard.sum_timings(timings, build_dashboard.READ_PHASES)
    return float(timings.get("scan", 0.0))


def flatten_row(raw_row: dict[str, Any], run_id: str, summary_path: Path) -> dict[str, Any]:
    row = dict(raw_row)
    row["engine"] = str(row.get("engine", "duckdb"))
    row["variant"] = str(row.get("variant", "default"))
    row["workload"] = str(row.get("workload", "crud"))
    timings = dict(row.get("timings", {}))
    phase_timings = build_dashboard.summarize_phase_timings(timings)
    output = {
        "run_id": run_id,
        "summary_path": summary_path.relative_to(ROOT).as_posix()
        if summary_path.is_relative_to(ROOT)
        else str(summary_path),
        "catalog": str(row.get("target", "")),
        "catalog_label": build_dashboard.TARGET_LABELS.get(
            str(row.get("target", "")), str(row.get("target", ""))
        ),
        "engine": row["engine"],
        "workload": row["workload"],
        "variant": row["variant"],
        "size": str(row.get("size", "")),
        "rows": int(row.get("rows") or 0),
        "repetition": int(row.get("repetition") or 0),
        "passed": bool(row.get("passed")),
        "error": str(row.get("error", "")),
        "table": str(row.get("table", "")),
        "total_s": round(build_dashboard.sum_timings(timings), 6),
        "workload_wall_s": round(
            build_dashboard.sum_timings(timings, build_dashboard.WORKLOAD_PHASES), 6
        ),
        "operation_s": round(operation_seconds(row, phase_timings), 6),
        "read_s": round(read_seconds(row, timings), 6),
        "support_s": round(build_dashboard.sum_timings(timings, build_dashboard.SUPPORT_PHASES), 6),
        "http_duration_ms": int(row.get("http_duration_ms") or 0),
        "http_request_count": int(row.get("http_request_count") or 0),
        "read_metrics_json": json_text(row.get("read_metrics")),
        "http_groups_json": json_text(row.get("http_groups")),
        "http_phase_groups_json": json_text(row.get("http_phase_groups")),
        "timings_json": json_text(timings),
        "raw_result_json": json_text(row),
    }
    for phase in PHASE_COLUMNS:
        output[f"{phase}_s"] = round(float(phase_timings.get(phase, 0.0)), 6)
    return output


def load_rows(run_ids: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for run_id in run_ids:
        run_root = ROOT / ".tmp" / "catalog_benchmarks" / run_id
        if not run_root.exists():
            raise SystemExit(f"run root does not exist: {run_root}")
        for summary_path in sorted(run_root.rglob("summary.json")):
            for raw_row in json.loads(summary_path.read_text()):
                rows.append(flatten_row(raw_row, run_id, summary_path))
    return sorted(rows, key=lambda row: (row["rows"], row["catalog"], row["engine"]))


def write_parquet(rows: list[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, output_path)


def fmt_seconds(value: float) -> str:
    return f"{value:.3f}s"


def markdown_table(headers: list[str], rows: list[list[Any]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(value) for value in row) + " |")
    return "\n".join(lines)


def fastest_rows(rows: list[dict[str, Any]]) -> list[list[str]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["size"], row["catalog"])].append(row)

    output = []
    for (size, catalog), entries in sorted(
        grouped.items(), key=lambda item: (item[0][0], item[0][1])
    ):
        best = min(entries, key=lambda row: row["operation_s"])
        worst = max(entries, key=lambda row: row["operation_s"])
        spread = worst["operation_s"] / best["operation_s"] if best["operation_s"] else 0
        output.append(
            [
                size,
                catalog,
                f"{best['engine']} {fmt_seconds(best['operation_s'])}",
                f"{worst['engine']} {fmt_seconds(worst['operation_s'])}",
                f"{spread:.2f}x",
            ]
        )
    return output


def write_report(rows: list[dict[str, Any]], report_path: Path, parquet_path: Path) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    by_size = defaultdict(int)
    for row in rows:
        by_size[row["size"]] += 1

    all_rows = [
        [
            row["size"],
            row["rows"],
            row["catalog"],
            row["engine"],
            row["workload"],
            row["variant"],
            "yes" if row["passed"] else "no",
            fmt_seconds(row["total_s"]),
            fmt_seconds(row["operation_s"]),
            fmt_seconds(row["read_s"]),
            row["http_duration_ms"],
            row["http_request_count"],
        ]
        for row in rows
    ]

    lines = [
        "# Engine matrix combined report",
        "",
        f"Rows: {len(rows)} benchmark result rows.",
        f"Parquet: `{parquet_path.relative_to(ROOT).as_posix()}`",
        "",
        "Included run roots:",
        "",
    ]
    for run_id in sorted({row["run_id"] for row in rows}):
        lines.append(f"- `.tmp/catalog_benchmarks/{run_id}`")

    lines.extend(
        [
            "",
            (
                "Important comparison note: DuckDB uses the existing CRUD workload, while "
                "PyIceberg and Spark use create-write-read. The `operation_s` column excludes "
                "engine startup/setup phases where possible, but DuckDB still includes delete "
                "and read-after-delete work."
            ),
            "",
            "## Row counts by size",
            "",
            markdown_table(
                ["size", "benchmark_rows"],
                [[size, count] for size, count in sorted(by_size.items())],
            ),
            "",
            "## Fastest operation by size and catalog",
            "",
            markdown_table(["size", "catalog", "fastest", "slowest", "spread"], fastest_rows(rows)),
            "",
            "## All benchmark results",
            "",
            markdown_table(
                [
                    "size",
                    "rows",
                    "catalog",
                    "engine",
                    "workload",
                    "variant",
                    "passed",
                    "total_s",
                    "operation_s",
                    "read_s",
                    "http_ms",
                    "requests",
                ],
                all_rows,
            ),
            "",
            "## Parquet columns",
            "",
            (
                "The Parquet file includes the visible report columns plus source metadata, "
                "phase timing columns, raw timing JSON, HTTP group JSON, read metric JSON, "
                "and the raw summary row JSON."
            ),
        ]
    )
    report_path.write_text("\n".join(lines) + "\n")


def html_escape(value: Any) -> str:
    text = str(value)
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def html_table(headers: list[str], rows: list[list[Any]]) -> str:
    head = "".join(f"<th>{html_escape(header)}</th>" for header in headers)
    body = "\n".join(
        "<tr>" + "".join(f"<td>{html_escape(value)}</td>" for value in row) + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def write_html_report(rows: list[dict[str, Any]], html_path: Path, parquet_path: Path) -> None:
    html_path.parent.mkdir(parents=True, exist_ok=True)
    by_size = defaultdict(int)
    for row in rows:
        by_size[row["size"]] += 1

    all_rows = [
        [
            row["size"],
            row["rows"],
            row["catalog"],
            row["engine"],
            row["workload"],
            row["variant"],
            "yes" if row["passed"] else "no",
            fmt_seconds(row["total_s"]),
            fmt_seconds(row["operation_s"]),
            fmt_seconds(row["read_s"]),
            row["http_duration_ms"],
            row["http_request_count"],
        ]
        for row in rows
    ]
    run_items = "\n".join(
        f"<li><code>.tmp/catalog_benchmarks/{html_escape(run_id)}</code></li>"
        for run_id in sorted({row["run_id"] for row in rows})
    )
    row_count_table = html_table(
        ["size", "benchmark_rows"],
        [[size, count] for size, count in sorted(by_size.items())],
    )
    fastest_table = html_table(
        ["size", "catalog", "fastest", "slowest", "spread"],
        fastest_rows(rows),
    )
    result_headers = [
        "size",
        "rows",
        "catalog",
        "engine",
        "workload",
        "variant",
        "passed",
        "total_s",
        "operation_s",
        "read_s",
        "http_ms",
        "requests",
    ]
    result_table = html_table(result_headers, all_rows)
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Engine matrix combined report</title>
  <style>
    body {{
      margin: 24px;
      color: #17202a;
      background: #f7f8fb;
      font: 14px/1.4 ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont,
        "Segoe UI", sans-serif;
    }}
    main {{ max-width: 1280px; margin: 0 auto; }}
    h1, h2 {{ line-height: 1.15; }}
    code {{ background: #edf2f6; padding: 1px 4px; border-radius: 4px; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin: 12px 0 28px;
      background: #fff;
      box-shadow: 0 8px 20px rgba(24, 37, 56, 0.06);
    }}
    th, td {{
      padding: 7px 9px;
      border-bottom: 1px solid #d6dde6;
      text-align: left;
      white-space: nowrap;
    }}
    th {{ background: #edf2f6; position: sticky; top: 0; z-index: 1; }}
    .table-wrap {{ overflow-x: auto; }}
    .note {{
      max-width: 920px;
      padding: 10px 12px;
      background: #fff7e6;
      border: 1px solid #e3c16d;
      border-radius: 6px;
    }}
  </style>
</head>
<body>
<main>
  <h1>Engine matrix combined report</h1>
  <p>Rows: {len(rows)} benchmark result rows.</p>
  <p>Parquet: <code>{html_escape(parquet_path.relative_to(ROOT).as_posix())}</code></p>
  <h2>Included run roots</h2>
  <ul>{run_items}</ul>
  <p class="note">DuckDB uses the existing CRUD workload, while PyIceberg and Spark
  use create-write-read. The <code>operation_s</code> column excludes engine
  startup/setup phases where possible, but DuckDB still includes delete and
  read-after-delete work.</p>
  <h2>Row counts by size</h2>
  <div class="table-wrap">
    {row_count_table}
  </div>
  <h2>Fastest operation by size and catalog</h2>
  <div class="table-wrap">
    {fastest_table}
  </div>
  <h2>All benchmark results</h2>
  <div class="table-wrap">
    {result_table}
  </div>
</main>
</body>
</html>
"""
    html_path.write_text(html)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", action="append", dest="run_ids")
    parser.add_argument(
        "--report",
        type=Path,
        default=ROOT / "reports" / "engine-matrix-all-20260626.md",
    )
    parser.add_argument(
        "--parquet",
        type=Path,
        default=ROOT / "reports" / "engine-matrix-all-20260626.parquet",
    )
    parser.add_argument(
        "--html",
        type=Path,
        default=ROOT / "reports" / "engine-matrix-all-20260626.html",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = load_rows(args.run_ids or list(DEFAULT_RUN_IDS))
    write_parquet(rows, args.parquet)
    write_report(rows, args.report, args.parquet)
    write_html_report(rows, args.html, args.parquet)
    print(f"wrote {args.report.relative_to(ROOT)}")
    print(f"wrote {args.parquet.relative_to(ROOT)}")
    print(f"wrote {args.html.relative_to(ROOT)}")
    print(f"rows {len(rows)}")


if __name__ == "__main__":
    main()
