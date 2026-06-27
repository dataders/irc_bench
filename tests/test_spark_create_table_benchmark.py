"""Unit tests for the Spark create/write/read benchmark runner.

Run: uv run tests/test_spark_create_table_benchmark.py -v
"""

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "spark_create_table_benchmark.py"


def load_module():
    spec = importlib.util.spec_from_file_location("spark_create_table_benchmark", MODULE_PATH)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class SparkCreateTableBenchmarkTest(unittest.TestCase):
    def setUp(self):
        self.bench = load_module()
        self.catalog_bench = self.bench.catalog_benchmark

    def test_spark_conf_maps_aws_s3_tables_rest_catalog(self):
        env = {
            "AWS_ACCESS_KEY_ID": "fake-aws-key",
            "AWS_SECRET_ACCESS_KEY": "fake-aws-secret",
            "AWS_S3_TABLES_BUCKET_ARN": "arn:aws:s3tables:us-west-2:123456789012:bucket/demo",
            "AWS_S3_TABLES_REGION": "us-west-2",
        }
        target = self.catalog_bench.load_targets(env)["aws_s3_tables"]

        conf = self.bench.spark_catalog_conf(target, env)

        self.assertEqual(conf["spark.sql.catalog.bench"], "org.apache.iceberg.spark.SparkCatalog")
        self.assertEqual(conf["spark.sql.catalog.bench.type"], "rest")
        self.assertEqual(
            conf["spark.sql.catalog.bench.uri"], "https://s3tables.us-west-2.amazonaws.com/iceberg"
        )
        self.assertEqual(conf["spark.sql.catalog.bench.rest.sigv4-enabled"], "true")
        self.assertEqual(conf["spark.sql.catalog.bench.rest.signing-name"], "s3tables")
        self.assertEqual(conf["spark.sql.catalog.bench.rest.signing-region"], "us-west-2")
        self.assertEqual(
            conf["spark.sql.catalog.bench.io-impl"],
            "org.apache.iceberg.aws.s3.S3FileIO",
        )

    def test_spark_conf_maps_aws_glue_catalog(self):
        env = {
            "AWS_ACCESS_KEY_ID": "fake-aws-key",
            "AWS_SECRET_ACCESS_KEY": "fake-aws-secret",
            "AWS_GLUE_REST_ACCOUNT_ID": "123456789012",
            "AWS_GLUE_REST_REGION": "us-west-2",
            "AWS_GLUE_REST_TABLE_LOCATION_ROOT": "s3://bucket/glue",
        }
        target = self.catalog_bench.load_targets(env)["aws_glue"]

        conf = self.bench.spark_catalog_conf(target, env)

        self.assertEqual(conf["spark.sql.catalog.bench"], "org.apache.iceberg.spark.SparkCatalog")
        self.assertEqual(
            conf["spark.sql.catalog.bench.catalog-impl"],
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        self.assertEqual(conf["spark.sql.catalog.bench.glue.id"], "123456789012")
        self.assertEqual(conf["spark.sql.catalog.bench.warehouse"], "s3://bucket/glue")
        self.assertEqual(
            conf["spark.sql.catalog.bench.io-impl"],
            "org.apache.iceberg.aws.s3.S3FileIO",
        )

    def test_spark_conf_maps_oauth_rest_catalog(self):
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

        conf = self.bench.spark_catalog_conf(target, env)

        self.assertEqual(conf["spark.sql.catalog.bench.type"], "rest")
        self.assertEqual(conf["spark.sql.catalog.bench.uri"], "https://polaris.example/api/catalog")
        self.assertEqual(conf["spark.sql.catalog.bench.credential"], "client-id:client-secret")
        self.assertEqual(
            conf["spark.sql.catalog.bench.oauth2-server-uri"],
            "https://polaris.example/oauth/tokens",
        )
        self.assertEqual(conf["spark.sql.catalog.bench.scope"], "PRINCIPAL_ROLE:ALL")
        self.assertEqual(
            conf["spark.sql.catalog.bench.header.X-Iceberg-Access-Delegation"],
            "vended-credentials",
        )

    def test_table_identifier_uses_spark_catalog_alias(self):
        target = self.catalog_bench.load_targets()["lakekeeper_local"]
        size = self.catalog_bench.BenchmarkSize("tiny", 4)

        self.assertEqual(
            self.bench.table_identifier(target, size, repetition=2),
            "bench.default.bench_spark_create_table_tiny_r2",
        )

    def test_drop_table_sql_uses_purge_for_s3_tables(self):
        env = {
            "AWS_ACCESS_KEY_ID": "fake-aws-key",
            "AWS_SECRET_ACCESS_KEY": "fake-aws-secret",
            "AWS_S3_TABLES_BUCKET_ARN": "arn:aws:s3tables:us-west-2:123456789012:bucket/demo",
        }
        target = self.catalog_bench.load_targets(env)["aws_s3_tables"]
        size = self.catalog_bench.BenchmarkSize("tiny", 4)

        self.assertEqual(
            self.bench.drop_table_sql(target, size, repetition=1),
            "DROP TABLE IF EXISTS bench.irc_duckdb_bench.bench_spark_create_table_tiny_r1 PURGE",
        )


if __name__ == "__main__":
    unittest.main()
