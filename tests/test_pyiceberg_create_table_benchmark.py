"""Unit tests for the PyIceberg create-table benchmark runner.

Run: uv run tests/test_pyiceberg_create_table_benchmark.py -v
"""

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "pyiceberg_create_table_benchmark.py"


def load_module():
    spec = importlib.util.spec_from_file_location("pyiceberg_create_table_benchmark", MODULE_PATH)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class PyIcebergCreateTableBenchmarkTest(unittest.TestCase):
    def setUp(self):
        self.bench = load_module()
        self.catalog_bench = self.bench.catalog_benchmark

    def test_catalog_properties_map_lakekeeper_s3_file_io(self):
        env = {
            "LAKEKEEPER_S3_KEY_ID": "minio-root-user",
            "LAKEKEEPER_S3_SECRET": "minio-root-password",
        }
        target = self.catalog_bench.load_targets(env)["lakekeeper_local"]

        properties = self.bench.pyiceberg_catalog_properties(target)

        self.assertEqual(properties["type"], "rest")
        self.assertEqual(properties["uri"], "http://localhost:18181/catalog")
        self.assertEqual(properties["warehouse"], "demo")
        self.assertEqual(properties["auth.type"], "noop")
        self.assertEqual(properties["s3.endpoint"], "http://localhost:19000")
        self.assertEqual(properties["s3.access-key-id"], "minio-root-user")
        self.assertEqual(properties["s3.secret-access-key"], "minio-root-password")
        self.assertEqual(properties["s3.force-virtual-addressing"], "false")
        self.assertEqual(properties["s3.region"], "us-east-1")

    def test_catalog_properties_map_oauth_and_vended_credentials(self):
        env = {
            "POLARIS_URL": "https://polaris.example/api/catalog",
            "POLARIS_WAREHOUSE": "warehouse",
            "POLARIS_ID": "client-id",
            "POLARIS_SECRET": "client-secret",
            "POLARIS_OAUTH_TOKEN_URI": "https://polaris.example/oauth/tokens",
            "POLARIS_OAUTH_SCOPE": "PRINCIPAL_ROLE:ALL",
            "POLARIS_DEFAULT_REGION": "us-west-2",
        }
        target = self.catalog_bench.load_targets(env)["polaris_remote"]

        properties = self.bench.pyiceberg_catalog_properties(target)

        self.assertEqual(properties["credential"], "client-id:client-secret")
        self.assertEqual(properties["oauth2-server-uri"], "https://polaris.example/oauth/tokens")
        self.assertEqual(properties["scope"], "PRINCIPAL_ROLE:ALL")
        self.assertEqual(properties["header.X-Iceberg-Access-Delegation"], "vended-credentials")
        self.assertEqual(properties["client.region"], "us-west-2")
        self.assertEqual(properties["py-io-impl"], "pyiceberg.io.fsspec.FsspecFileIO")

    def test_catalog_properties_map_static_token(self):
        env = {
            "HORIZON_ENDPOINT": "https://acct.snowflakecomputing.com/polaris/api/catalog",
            "HORIZON_WAREHOUSE": "CODEX_HORIZON_DEMO",
            "HORIZON_ACCESS_TOKEN": "access-token",
            "HORIZON_SCHEMA": "AWS_CLOUD_COST",
            "SNOWFLAKE_DEFAULT_REGION": "us-east-1",
        }
        target = self.catalog_bench.load_targets(env)["horizon"]

        properties = self.bench.pyiceberg_catalog_properties(target)

        self.assertEqual(properties["token"], "access-token")
        self.assertEqual(properties["warehouse"], "CODEX_HORIZON_DEMO")
        self.assertEqual(properties["client.region"], "us-east-1")

    def test_catalog_properties_map_aws_glue_native_catalog(self):
        env = {
            "AWS_ACCESS_KEY_ID": "fake-aws-key",
            "AWS_SECRET_ACCESS_KEY": "fake-aws-secret",
            "AWS_SESSION_TOKEN": "fake-session",
            "AWS_GLUE_REST_ACCOUNT_ID": "123456789012",
            "AWS_GLUE_REST_REGION": "us-west-2",
            "AWS_GLUE_REST_TABLE_LOCATION_ROOT": "s3://bucket/glue",
        }
        target = self.catalog_bench.load_targets(env)["aws_glue"]

        properties = self.bench.pyiceberg_catalog_properties(target, env)

        self.assertEqual(properties["type"], "glue")
        self.assertEqual(properties["warehouse"], "s3://bucket/glue")
        self.assertEqual(properties["glue.id"], "123456789012")
        self.assertEqual(properties["glue.region"], "us-west-2")
        self.assertEqual(properties["client.access-key-id"], "fake-aws-key")
        self.assertEqual(properties["client.secret-access-key"], "fake-aws-secret")
        self.assertEqual(properties["client.session-token"], "fake-session")

    def test_catalog_properties_map_aws_s3_tables_rest_catalog(self):
        env = {
            "AWS_ACCESS_KEY_ID": "fake-aws-key",
            "AWS_SECRET_ACCESS_KEY": "fake-aws-secret",
            "AWS_S3_TABLES_BUCKET_ARN": "arn:aws:s3tables:us-west-2:123456789012:bucket/demo",
            "AWS_S3_TABLES_REGION": "us-west-2",
        }
        target = self.catalog_bench.load_targets(env)["aws_s3_tables"]

        properties = self.bench.pyiceberg_catalog_properties(target, env)

        self.assertEqual(properties["type"], "rest")
        self.assertEqual(properties["uri"], "https://s3tables.us-west-2.amazonaws.com/iceberg")
        self.assertEqual(
            properties["warehouse"],
            "arn:aws:s3tables:us-west-2:123456789012:bucket/demo",
        )
        self.assertEqual(properties["rest.sigv4-enabled"], "true")
        self.assertEqual(properties["rest.signing-name"], "s3tables")
        self.assertEqual(properties["rest.signing-region"], "us-west-2")

    def test_table_location_uses_glue_root_when_required(self):
        env = {
            "AWS_ACCESS_KEY_ID": "fake-aws-key",
            "AWS_SECRET_ACCESS_KEY": "fake-aws-secret",
            "AWS_GLUE_REST_ACCOUNT_ID": "123456789012",
            "AWS_GLUE_REST_TABLE_LOCATION_ROOT": "s3://bucket/glue",
        }
        target = self.catalog_bench.load_targets(env)["aws_glue"]
        size = self.catalog_bench.BenchmarkSize("tiny", 4)

        self.assertEqual(
            self.bench.table_location(target, size, repetition=1),
            "s3://bucket/glue/bench_pyiceberg_create_table_tiny_r1/",
        )

    def test_s3_tables_drop_uses_purge(self):
        class FakeCatalog:
            def __init__(self):
                self.calls = []

            def drop_table(self, identifier):
                self.calls.append(("drop_table", identifier))

            def purge_table(self, identifier):
                self.calls.append(("purge_table", identifier))

        env = {
            "AWS_ACCESS_KEY_ID": "fake-aws-key",
            "AWS_SECRET_ACCESS_KEY": "fake-aws-secret",
            "AWS_S3_TABLES_BUCKET_ARN": "arn:aws:s3tables:us-west-2:123456789012:bucket/demo",
        }
        target = self.catalog_bench.load_targets(env)["aws_s3_tables"]
        catalog = FakeCatalog()
        timings = {}

        self.bench.drop_table(catalog, target, ("irc_duckdb_bench", "table"), timings, "cleanup")

        self.assertEqual(catalog.calls, [("purge_table", ("irc_duckdb_bench", "table"))])
        self.assertIn("cleanup", timings)

    def test_identifier_uses_default_schema_and_size_label(self):
        target = self.catalog_bench.load_targets()["lakekeeper_local"]
        size = self.catalog_bench.BenchmarkSize("tiny", 4)

        self.assertEqual(
            self.bench.table_identifier(target, size, repetition=2),
            ("default", "bench_pyiceberg_create_table_tiny_r2"),
        )

    def test_benchmark_arrow_table_contains_expected_rows(self):
        size = self.catalog_bench.BenchmarkSize("tiny", 4)

        table = self.bench.benchmark_arrow_table(size)

        self.assertEqual(table.num_rows, 4)
        self.assertEqual(table.column("id").to_pylist(), [0, 1, 2, 3])
        self.assertEqual(table.column("h").to_pylist(), [0, 654435747, 308871487, 963307234])
        self.assertEqual(
            table.column("label").to_pylist(),
            ["payload 0", "payload 1", "payload 2", "payload 3"],
        )

    def test_read_metrics_validate_written_rows(self):
        size = self.catalog_bench.BenchmarkSize("tiny", 4)

        metrics = self.bench.read_metrics(self.bench.benchmark_arrow_table(size), size)

        self.assertEqual(metrics, {"row_count": 4, "id_sum": 6})

    def test_summary_rows_flatten_phase_timings(self):
        rows = [
            {
                "target": "lakekeeper_local",
                "engine": "pyiceberg",
                "workload": "create-write-read",
                "size": "tiny",
                "repetition": 1,
                "passed": True,
                "timings": {"load_catalog": 0.1, "create_table": 0.2},
            }
        ]

        flattened = self.bench.csv_summary_rows(rows)

        self.assertEqual(
            flattened,
            [
                {
                    "target": "lakekeeper_local",
                    "engine": "pyiceberg",
                    "workload": "create-write-read",
                    "size": "tiny",
                    "repetition": 1,
                    "passed": True,
                    "phase": "load_catalog",
                    "wall_seconds": 0.1,
                },
                {
                    "target": "lakekeeper_local",
                    "engine": "pyiceberg",
                    "workload": "create-write-read",
                    "size": "tiny",
                    "repetition": 1,
                    "passed": True,
                    "phase": "create_table",
                    "wall_seconds": 0.2,
                },
            ],
        )


if __name__ == "__main__":
    unittest.main()
