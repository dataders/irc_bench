#!/usr/bin/env python3
"""Run a Spark Iceberg create-write-read benchmark."""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import re
import sys
import time
import traceback
from collections.abc import Callable
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = ROOT / ".tmp" / "catalog_benchmarks"
SPARK_CATALOG = "bench"
DEFAULT_SPARK_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.2,"
    "software.amazon.awssdk:bundle:2.20.160,"
    "software.amazon.awssdk:url-connection-client:2.20.160"
)


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


def aws_rest_endpoint(target: catalog_benchmark.CatalogTarget) -> str:
    if target.endpoint_type == "glue":
        return f"https://glue.{target.default_region}.amazonaws.com/iceberg"
    if target.endpoint_type == "s3_tables":
        return f"https://s3tables.{target.default_region}.amazonaws.com/iceberg"
    return target.endpoint


def base_spark_conf(env: dict[str, str]) -> dict[str, str]:
    return {
        "spark.jars.packages": env.get("ICEBERG_SPARK_PACKAGES", DEFAULT_SPARK_PACKAGES),
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": SPARK_CATALOG,
        "spark.driver.host": "127.0.0.1",
        "spark.ui.enabled": "false",
    }


def spark_catalog_conf(
    target: catalog_benchmark.CatalogTarget, env: dict[str, str]
) -> dict[str, str]:
    prefix = f"spark.sql.catalog.{SPARK_CATALOG}"
    conf = {
        prefix: "org.apache.iceberg.spark.SparkCatalog",
    }
    if target.endpoint_type == "glue":
        conf.update(
            {
                f"{prefix}.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
                f"{prefix}.glue.id": target.warehouse,
                f"{prefix}.warehouse": target.table_location_root or target.warehouse,
                f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            }
        )
        if target.default_region:
            conf[f"{prefix}.client.region"] = target.default_region
        return conf

    conf.update(
        {
            f"{prefix}.type": "rest",
            f"{prefix}.uri": aws_rest_endpoint(target),
            f"{prefix}.warehouse": target.warehouse,
        }
    )
    if target.endpoint_type == "s3_tables":
        conf.update(
            {
                f"{prefix}.rest.sigv4-enabled": "true",
                f"{prefix}.rest.signing-name": "s3tables",
                f"{prefix}.rest.signing-region": target.default_region or "",
                f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                f"{prefix}.rest-metrics-reporting-enabled": "false",
            }
        )
    if target.token_secret:
        conf[f"{prefix}.token"] = target.token_secret["token"]
    if target.oauth_secret:
        secret = target.oauth_secret
        conf[f"{prefix}.credential"] = f"{secret['client_id']}:{secret['client_secret']}"
        conf[f"{prefix}.oauth2-server-uri"] = secret["oauth2_server_uri"]
        conf[f"{prefix}.scope"] = secret.get("oauth2_scope", "PRINCIPAL_ROLE:ALL")
    if target.access_delegation_mode == "VENDED_CREDENTIALS":
        conf[f"{prefix}.header.X-Iceberg-Access-Delegation"] = "vended-credentials"
        conf[f"{prefix}.io-impl"] = "org.apache.iceberg.aws.s3.S3FileIO"
    if target.default_region:
        conf[f"{prefix}.client.region"] = target.default_region
    return conf


def table_basename(size: catalog_benchmark.BenchmarkSize, repetition: int) -> str:
    table = f"bench_spark_create_table_{size.label}_r{repetition}".lower()
    return re.sub(r"[^a-z0-9_]", "_", table)


def table_identifier(
    target: catalog_benchmark.CatalogTarget,
    size: catalog_benchmark.BenchmarkSize,
    repetition: int,
) -> str:
    return f"{SPARK_CATALOG}.{target.default_schema}.{table_basename(size, repetition)}"


def table_location(
    target: catalog_benchmark.CatalogTarget,
    size: catalog_benchmark.BenchmarkSize,
    repetition: int,
) -> str | None:
    if not target.table_location_root:
        return None
    return f"{target.table_location_root.rstrip('/')}/{table_basename(size, repetition)}/"


def timed(timings: dict[str, float], phase: str, func: Callable[[], Any]) -> Any:
    started_at = time.perf_counter()
    try:
        return func()
    finally:
        timings[phase] = timings.get(phase, 0.0) + (time.perf_counter() - started_at)


def purge_with_pyiceberg(
    target: catalog_benchmark.CatalogTarget,
    env: dict[str, str],
    size: catalog_benchmark.BenchmarkSize,
    repetition: int,
) -> None:
    import importlib.util

    module_path = ROOT / "scripts" / "pyiceberg_create_table_benchmark.py"
    spec = importlib.util.spec_from_file_location("pyiceberg_create_table_benchmark", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    catalog = module.load_catalog(
        name=f"{target.name}_spark_cleanup",
        **module.pyiceberg_catalog_properties(target, env),
    )
    identifier = target.default_schema, table_basename(size, repetition)
    with module.suppress(module.NoSuchTableError):
        catalog.purge_table(identifier)


def drop_table(
    spark: Any,
    target: catalog_benchmark.CatalogTarget,
    env: dict[str, str],
    size: catalog_benchmark.BenchmarkSize,
    repetition: int,
) -> None:
    if target.endpoint_type == "s3_tables":
        purge_with_pyiceberg(target, env, size, repetition)
        return
    spark.sql(drop_table_sql(target, size, repetition))


def create_spark_session(target: catalog_benchmark.CatalogTarget, env: dict[str, str]):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(f"irc-duckdb-bench-{target.name}").master("local[2]")
    for key, value in {**base_spark_conf(env), **spark_catalog_conf(target, env)}.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def create_table_sql(
    target: catalog_benchmark.CatalogTarget,
    size: catalog_benchmark.BenchmarkSize,
    repetition: int,
) -> str:
    sql = (
        f"CREATE TABLE {table_identifier(target, size, repetition)} "
        "(id BIGINT NOT NULL, h BIGINT, label STRING) USING iceberg"
    )
    location = table_location(target, size, repetition)
    if location:
        sql += f" LOCATION '{location}'"
    return sql


def drop_table_sql(
    target: catalog_benchmark.CatalogTarget,
    size: catalog_benchmark.BenchmarkSize,
    repetition: int,
) -> str:
    sql = f"DROP TABLE IF EXISTS {table_identifier(target, size, repetition)}"
    if target.endpoint_type == "s3_tables":
        sql += " PURGE"
    return sql


def expected_sum(size: catalog_benchmark.BenchmarkSize) -> int:
    return size.rows * (size.rows - 1) // 2


def benchmark_dataframe(spark: Any, size: catalog_benchmark.BenchmarkSize) -> Any:
    from pyspark.sql import functions as F

    id_column = F.col("id").cast("long")
    return spark.range(size.rows).select(
        id_column.alias("id"),
        ((id_column * F.lit(2_654_435_761)) % F.lit(1_000_000_007)).cast("long").alias("h"),
        F.concat(F.lit("payload "), id_column.cast("string")).alias("label"),
    )


def run_one(
    target: catalog_benchmark.CatalogTarget,
    env: dict[str, str],
    size: catalog_benchmark.BenchmarkSize,
    repetition: int,
    keep_tables: bool,
) -> dict[str, Any]:
    timings: dict[str, float] = {}
    identifier = table_identifier(target, size, repetition)
    try:
        spark = timed(timings, "start_spark", lambda: create_spark_session(target, env))
        if target.create_schema:
            namespace_sql = (
                f"CREATE NAMESPACE IF NOT EXISTS {SPARK_CATALOG}.{target.default_schema}"
            )
            timed(
                timings,
                "create_namespace",
                lambda: spark.sql(namespace_sql),
            )
        create_sql = create_table_sql(target, size, repetition)
        timed(
            timings,
            "drop_existing_table",
            lambda: drop_table(spark, target, env, size, repetition),
        )
        timed(timings, "create_table", lambda: spark.sql(create_sql))
        dataframe = benchmark_dataframe(spark, size)
        timed(timings, "append", lambda: dataframe.writeTo(identifier).append())
        metrics_row = timed(
            timings,
            "scan",
            lambda: spark.sql(
                f"SELECT count(*) AS row_count, sum(id) AS id_sum FROM {identifier}"
            ).collect()[0],
        )
        row_count = int(metrics_row["row_count"])
        id_sum = int(metrics_row["id_sum"] or 0)
        if row_count != size.rows or id_sum != expected_sum(size):
            raise ValueError(
                f"read verification failed: row_count={row_count}, id_sum={id_sum}, "
                f"expected row_count={size.rows}, expected id_sum={expected_sum(size)}"
            )
        if not keep_tables:
            timed(timings, "cleanup", lambda: drop_table(spark, target, env, size, repetition))
        return {
            "target": target.name,
            "engine": "spark",
            "workload": "create-write-read",
            "size": size.label,
            "rows": size.rows,
            "repetition": repetition,
            "table": identifier,
            "passed": True,
            "error": "",
            "read_metrics": {"row_count": row_count, "id_sum": id_sum},
            "timings": timings,
        }
    except Exception as exc:
        return {
            "target": target.name,
            "engine": "spark",
            "workload": "create-write-read",
            "size": size.label,
            "rows": size.rows,
            "repetition": repetition,
            "table": identifier,
            "passed": False,
            "error": str(exc),
            "error_type": type(exc).__name__,
            "timings": timings,
            "traceback": traceback.format_exc(limit=8),
        }
    finally:
        session = sys.modules.get("pyspark.sql.session")
        if session is not None:
            spark_session = getattr(session, "SparkSession", None)
            active_session = getattr(spark_session, "getActiveSession", lambda: None)()
            if active_session is not None:
                active_session.stop()


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
    parser.add_argument("--target", required=False)
    parser.add_argument("--list-targets", action="store_true")
    parser.add_argument("--sizes", default="tiny")
    parser.add_argument("--rows")
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--keep-tables", action="store_true")
    parser.add_argument("--run-id")
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
    output_dir = OUTPUT_ROOT / run_id / target.name / "spark-create-write-read"
    rows = [
        run_one(target, env, size, repetition, args.keep_tables)
        for size in sizes
        for repetition in range(1, args.repetitions + 1)
    ]
    write_summary(rows, output_dir, env)
    print(output_dir)
    return 0 if all(row["passed"] for row in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
