#!/usr/bin/env python3
"""Run a PyIceberg REST catalog create-write-read benchmark.

This runner reuses the DuckDB benchmark target matrix, but measures PyIceberg
catalog and data-write operations directly instead of routing through DuckDB SQL.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import sys
import time
import traceback
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchTableError,
)
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = ROOT / ".tmp" / "catalog_benchmarks"


def load_catalog_benchmark_module():
    module_path = ROOT / "scripts" / "catalog_benchmark.py"
    spec = importlib.util.spec_from_file_location("catalog_benchmark", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


catalog_benchmark = load_catalog_benchmark_module()


def aws_env_properties(env: dict[str, str]) -> dict[str, str]:
    properties = {}
    if env.get("AWS_ACCESS_KEY_ID"):
        properties["client.access-key-id"] = env["AWS_ACCESS_KEY_ID"]
    if env.get("AWS_SECRET_ACCESS_KEY"):
        properties["client.secret-access-key"] = env["AWS_SECRET_ACCESS_KEY"]
    if env.get("AWS_SESSION_TOKEN"):
        properties["client.session-token"] = env["AWS_SESSION_TOKEN"]
    return properties


def aws_rest_endpoint(target: catalog_benchmark.CatalogTarget) -> str:
    if target.endpoint_type == "glue":
        return f"https://glue.{target.default_region}.amazonaws.com/iceberg"
    if target.endpoint_type == "s3_tables":
        return f"https://s3tables.{target.default_region}.amazonaws.com/iceberg"
    return target.endpoint


def pyiceberg_catalog_properties(
    target: catalog_benchmark.CatalogTarget, env: dict[str, str] | None = None
) -> dict[str, str]:
    env = env or catalog_benchmark.merged_env()
    if target.endpoint_type == "glue":
        properties = {
            "type": "glue",
            "warehouse": target.table_location_root or target.warehouse,
            "glue.id": target.warehouse,
        }
        if target.default_region:
            properties["glue.region"] = target.default_region
            properties["client.region"] = target.default_region
            properties["s3.region"] = target.default_region
        properties.update(aws_env_properties(env))
        return properties

    properties = {
        "type": "rest",
        "uri": aws_rest_endpoint(target),
        "warehouse": target.warehouse,
    }
    if target.endpoint_type == "s3_tables":
        properties["rest.sigv4-enabled"] = "true"
        properties["rest.signing-name"] = "s3tables"
        properties["rest.signing-region"] = target.default_region or ""
        properties["py-io-impl"] = "pyiceberg.io.fsspec.FsspecFileIO"
        properties.update(aws_env_properties(env))
    if target.default_region:
        properties["client.region"] = target.default_region
        properties["s3.region"] = target.default_region
    if target.access_delegation_mode == "VENDED_CREDENTIALS":
        properties["header.X-Iceberg-Access-Delegation"] = "vended-credentials"
        properties["py-io-impl"] = "pyiceberg.io.fsspec.FsspecFileIO"
    if target.authorization_type == "NONE":
        properties["auth.type"] = "noop"
    if target.token_secret:
        properties["token"] = target.token_secret["token"]
    if target.oauth_secret:
        secret = target.oauth_secret
        properties["credential"] = f"{secret['client_id']}:{secret['client_secret']}"
        properties["oauth2-server-uri"] = secret["oauth2_server_uri"]
        properties["scope"] = secret.get("oauth2_scope", "PRINCIPAL_ROLE:ALL")
    if target.s3_secret:
        secret = target.s3_secret
        scheme = "https" if secret.get("use_ssl") else "http"
        properties["s3.endpoint"] = f"{scheme}://{secret['endpoint']}"
        properties["s3.access-key-id"] = secret["key_id"]
        properties["s3.secret-access-key"] = secret["secret"]
        properties["s3.force-virtual-addressing"] = "false"
        properties.setdefault("s3.region", target.default_region or "us-east-1")
    return properties


def benchmark_schema() -> Schema:
    return Schema(
        NestedField(id=1, name="id", type=LongType(), required=True),
        NestedField(id=2, name="h", type=LongType(), required=False),
        NestedField(id=3, name="label", type=StringType(), required=False),
    )


def benchmark_arrow_table(size: catalog_benchmark.BenchmarkSize) -> pa.Table:
    ids = list(range(size.rows))
    return pa.Table.from_pydict(
        {
            "id": ids,
            "h": [(value * 2_654_435_761) % 1_000_000_007 for value in ids],
            "label": [f"payload {value}" for value in ids],
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("h", pa.int64()),
                pa.field("label", pa.string()),
            ]
        ),
    )


def read_metrics(table: pa.Table, size: catalog_benchmark.BenchmarkSize) -> dict[str, int]:
    row_count = table.num_rows
    id_sum = sum(table.column("id").to_pylist())
    expected_sum = size.rows * (size.rows - 1) // 2
    if row_count != size.rows or id_sum != expected_sum:
        raise ValueError(
            f"read verification failed: row_count={row_count}, id_sum={id_sum}, "
            f"expected row_count={size.rows}, expected id_sum={expected_sum}"
        )
    return {"row_count": row_count, "id_sum": id_sum}


def table_identifier(
    target: catalog_benchmark.CatalogTarget,
    size: catalog_benchmark.BenchmarkSize,
    repetition: int,
) -> tuple[str, str]:
    table = f"bench_pyiceberg_create_table_{size.label}_r{repetition}".lower()
    return target.default_schema, table


def table_location(
    target: catalog_benchmark.CatalogTarget,
    size: catalog_benchmark.BenchmarkSize,
    repetition: int,
) -> str | None:
    if not target.table_location_root:
        return None
    table_name = table_identifier(target, size, repetition)[1]
    return f"{target.table_location_root.rstrip('/')}/{table_name}/"


def timed(timings: dict[str, float], phase: str, func: Callable[[], Any]) -> Any:
    started_at = time.perf_counter()
    try:
        return func()
    finally:
        timings[phase] = timings.get(phase, 0.0) + (time.perf_counter() - started_at)


def create_namespace_if_needed(
    catalog: Catalog, target: catalog_benchmark.CatalogTarget, timings: dict[str, float]
) -> None:
    if not target.create_schema:
        return
    with suppress(NamespaceAlreadyExistsError):
        timed(timings, "create_namespace", lambda: catalog.create_namespace(target.default_schema))


def drop_table(
    catalog: Catalog,
    target: catalog_benchmark.CatalogTarget,
    identifier: tuple[str, str],
    timings: dict[str, float],
    phase: str,
) -> None:
    with suppress(NoSuchTableError):
        if target.endpoint_type == "s3_tables":
            timed(timings, phase, lambda: catalog.purge_table(identifier))
            return
        timed(timings, phase, lambda: catalog.drop_table(identifier))


def cleanup_table(
    catalog: Catalog,
    target: catalog_benchmark.CatalogTarget,
    identifier: tuple[str, str],
    timings: dict[str, float],
    keep_tables: bool,
) -> None:
    if keep_tables:
        return
    drop_table(catalog, target, identifier, timings, "cleanup")


def run_one(
    target: catalog_benchmark.CatalogTarget,
    size: catalog_benchmark.BenchmarkSize,
    repetition: int,
    keep_tables: bool,
) -> dict[str, Any]:
    timings: dict[str, float] = {}
    identifier = table_identifier(target, size, repetition)
    properties = pyiceberg_catalog_properties(target)
    try:
        catalog = timed(
            timings,
            "load_catalog",
            lambda: load_catalog(name=f"{target.name}_pyiceberg", **properties),
        )
        create_namespace_if_needed(catalog, target, timings)
        drop_table(catalog, target, identifier, timings, "drop_existing_table")
        timed(
            timings,
            "create_table",
            lambda: catalog.create_table(
                identifier=identifier,
                schema=benchmark_schema(),
                location=table_location(target, size, repetition),
            ),
        )
        table = timed(timings, "load_table", lambda: catalog.load_table(identifier))
        timed(timings, "append", lambda: table.append(benchmark_arrow_table(size)))
        readback = timed(
            timings,
            "scan",
            lambda: catalog.load_table(identifier).scan().to_arrow(),
        )
        metrics = read_metrics(readback, size)
        cleanup_table(catalog, target, identifier, timings, keep_tables)
        return {
            "target": target.name,
            "engine": "pyiceberg",
            "workload": "create-write-read",
            "size": size.label,
            "rows": size.rows,
            "repetition": repetition,
            "table": ".".join(identifier),
            "passed": True,
            "error": "",
            "read_metrics": metrics,
            "timings": timings,
        }
    except Exception as exc:
        return {
            "target": target.name,
            "engine": "pyiceberg",
            "workload": "create-write-read",
            "size": size.label,
            "rows": size.rows,
            "repetition": repetition,
            "table": ".".join(identifier),
            "passed": False,
            "error": str(exc),
            "error_type": type(exc).__name__,
            "timings": timings,
            "traceback": traceback.format_exc(limit=8),
        }


def csv_summary_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flattened = []
    for row in rows:
        base = {key: value for key, value in row.items() if key not in {"timings", "traceback"}}
        timings = row.get("timings") or {"": ""}
        for phase, seconds in timings.items():
            flattened.append({**base, "phase": phase, "wall_seconds": seconds})
    return flattened


def write_summary(rows: list[dict[str, Any]], output_dir: Path, env: dict[str, str]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    redacted_rows = json.loads(catalog_benchmark.redact(json.dumps(rows, indent=2), env))
    (output_dir / "summary.json").write_text(json.dumps(redacted_rows, indent=2, sort_keys=True))
    csv_rows = csv_summary_rows(redacted_rows)
    if not csv_rows:
        (output_dir / "summary.csv").write_text("")
        return
    fieldnames = sorted({key for row in csv_rows for key in row})
    with (output_dir / "summary.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target", required=False, help="Target name from benchmarks/catalog_benchmarks.toml"
    )
    parser.add_argument(
        "--list-targets", action="store_true", help="List configured targets and exit"
    )
    parser.add_argument("--sizes", default="tiny", help="Comma-separated named sizes")
    parser.add_argument("--rows", help="Comma-separated explicit row counts; overrides --sizes")
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--keep-tables", action="store_true")
    parser.add_argument("--run-id", help="Stable output directory suffix")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    env = catalog_benchmark.merged_env()
    targets = catalog_benchmark.load_targets(env)
    if args.list_targets:
        for target in targets.values():
            print(f"{target.name}: {target.description}")
        return 0
    if not args.target:
        raise SystemExit("--target is required unless --list-targets is used")
    if args.target not in targets:
        valid = ", ".join(sorted(targets))
        raise SystemExit(f"unknown target {args.target!r}; valid targets: {valid}")

    target = targets[args.target]
    missing = catalog_benchmark.missing_env(target, env)
    if missing:
        raise SystemExit(f"missing required env vars for {target.name}: {', '.join(missing)}")

    try:
        sizes = catalog_benchmark.parse_size_matrix(args.sizes, args.rows)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    run_id = args.run_id or time.strftime("%Y%m%dT%H%M%S")
    output_dir = OUTPUT_ROOT / run_id / target.name / "pyiceberg-create-write-read"
    rows = [
        run_one(target, size, repetition, args.keep_tables)
        for size in sizes
        for repetition in range(1, args.repetitions + 1)
    ]
    write_summary(rows, output_dir, env)
    print(output_dir)
    return 0 if all(row["passed"] for row in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
