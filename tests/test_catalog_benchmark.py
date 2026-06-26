"""Unit tests for the raw DuckDB catalog benchmark harness.

Run: uv run tests/test_catalog_benchmark.py -v
"""

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "catalog_benchmark.py"


def load_module():
    spec = importlib.util.spec_from_file_location("catalog_benchmark", MODULE_PATH)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class CatalogBenchmarkTest(unittest.TestCase):
    def setUp(self):
        self.bench = load_module()

    def test_size_matrix_supports_named_defaults_and_explicit_rows(self):
        named = self.bench.parse_size_matrix("tiny,medium", None)
        self.assertEqual(
            [(size.label, size.rows) for size in named], [("tiny", 4), ("medium", 1_000_000)]
        )

        explicit = self.bench.parse_size_matrix(None, "7,42")
        self.assertEqual(
            [(size.label, size.rows) for size in explicit],
            [("rows_7", 7), ("rows_42", 42)],
        )

    def test_scale_factor_matrix_labels_and_estimates_lineitem_rows(self):
        parsed = self.bench.parse_scale_factor_matrix("0.01,0.1,1")

        self.assertEqual(
            [(size.label, size.rows, size.scale_factor) for size in parsed],
            [("sf_0_01", 60_012, 0.01), ("sf_0_1", 600_122, 0.1), ("sf_1", 6_001_215, 1.0)],
        )

    def test_attach_variants_cover_horizon_ablation_cases(self):
        variants = self.bench.ATTACH_VARIANTS
        for name in [
            "default",
            "no_stage_create",
            "no_stage_no_purge",
            "no_multi_commit",
            "skip_create_metadata_updates",
            "stage_multi_metadata",
            "no_cleanup_on_rollback",
            "legacy_without_stage_create",
            "legacy_full_compat",
        ]:
            with self.subTest(name=name):
                self.assertIn(name, variants)

        stage_multi_metadata = variants["stage_multi_metadata"].options
        self.assertEqual(stage_multi_metadata["STAGE_CREATE_TABLES"], "false")
        self.assertEqual(stage_multi_metadata["DISABLE_MULTI_TABLE_COMMIT"], "true")
        self.assertEqual(stage_multi_metadata["SKIP_CREATE_TABLE_METADATA_UPDATES"], "true")
        self.assertNotIn("REMOVE_FILES_ON_DELETE", stage_multi_metadata)
        self.assertNotIn("READ_ONLY", stage_multi_metadata)

        no_stage_options = variants["legacy_without_stage_create"].options
        self.assertEqual(no_stage_options["DISABLE_MULTI_TABLE_COMMIT"], "true")
        self.assertEqual(no_stage_options["SKIP_CREATE_TABLE_METADATA_UPDATES"], "true")
        self.assertEqual(no_stage_options["REMOVE_FILES_ON_DELETE"], "false")
        self.assertNotIn("STAGE_CREATE_TABLES", no_stage_options)
        self.assertNotIn("READ_ONLY", no_stage_options)

        legacy_options = variants["legacy_full_compat"].options
        self.assertEqual(legacy_options["STAGE_CREATE_TABLES"], "false")
        self.assertEqual(legacy_options["DISABLE_MULTI_TABLE_COMMIT"], "true")
        self.assertEqual(legacy_options["SKIP_CREATE_TABLE_METADATA_UPDATES"], "true")
        self.assertEqual(legacy_options["REMOVE_FILES_ON_DELETE"], "false")

    def test_target_missing_env_reports_only_required_names(self):
        target = self.bench.load_targets()["horizon"]
        missing = self.bench.missing_env(target, {"HORIZON_ENDPOINT": "https://example"})
        self.assertEqual(
            missing,
            [
                "HORIZON_WAREHOUSE",
                "HORIZON_ACCESS_TOKEN",
                "HORIZON_SCHEMA",
                "SNOWFLAKE_DEFAULT_REGION",
            ],
        )

    def test_target_default_variants_lock_simplest_working_configs(self):
        targets = self.bench.load_targets(
            {
                "LAKEKEEPER_S3_KEY_ID": "fake-lakekeeper-key",
                "LAKEKEEPER_S3_SECRET": "fake-lakekeeper-secret",
                "POLARIS_LOCAL_ID": "fake-local-polaris-id",
                "POLARIS_LOCAL_SECRET": "fake-local-polaris-secret",
                "POLARIS_URL": "https://polaris.example",
                "POLARIS_WAREHOUSE": "warehouse",
                "POLARIS_ID": "client-id",
                "POLARIS_SECRET": "secret",
                "POLARIS_OAUTH_TOKEN_URI": "https://polaris.example/v1/oauth/tokens",
                "HORIZON_ENDPOINT": "https://acct.snowflakecomputing.com/polaris/api/catalog",
                "HORIZON_WAREHOUSE": "warehouse",
                "HORIZON_ACCESS_TOKEN": "token",
                "HORIZON_SCHEMA": "AWS_CLOUD_COST",
                "SNOWFLAKE_DEFAULT_REGION": "us-east-1",
                "AWS_ACCESS_KEY_ID": "fake-aws-key",
                "AWS_SECRET_ACCESS_KEY": "fake-aws-secret",
                "AWS_GLUE_REST_ACCOUNT_ID": "123456789012",
                "AWS_GLUE_REST_TABLE_LOCATION_ROOT": "s3://bucket/glue",
                "AWS_S3_TABLES_BUCKET_ARN": "arn:aws:s3tables:us-west-2:123456789012:bucket/demo",
            }
        )

        self.assertEqual(targets["lakekeeper_local"].default_variant, "default")
        self.assertEqual(targets["polaris_local"].default_variant, "default")
        self.assertEqual(targets["polaris_remote"].default_variant, "default")
        self.assertEqual(targets["horizon"].default_variant, "stage_multi_metadata")
        self.assertEqual(targets["aws_glue"].default_variant, "no_stage_no_purge")
        self.assertEqual(targets["aws_s3_tables"].default_variant, "no_stage_create")
        for target in targets.values():
            self.assertIn(target.default_variant, self.bench.ATTACH_VARIANTS)

    def test_horizon_sql_uses_secret_name_and_legacy_options(self):
        env = {
            "HORIZON_ENDPOINT": "https://acct.snowflakecomputing.com/polaris/api/catalog",
            "HORIZON_WAREHOUSE": "CODEX_HORIZON_DEMO",
            "HORIZON_ACCESS_TOKEN": "super-secret-token",
            "HORIZON_SCHEMA": "AWS_CLOUD_COST",
            "SNOWFLAKE_DEFAULT_REGION": "us-east-1",
        }
        target = self.bench.load_targets(env)["horizon"]

        secret_sql = self.bench.render_secret_sql(target, env)
        attach_sql = self.bench.render_attach_sql(
            target, env, self.bench.ATTACH_VARIANTS["legacy_full_compat"]
        )

        self.assertIn("CREATE OR REPLACE SECRET snowflake_oauth", secret_sql)
        self.assertIn("TOKEN 'super-secret-token'", secret_sql)
        self.assertIn("ATTACH 'CODEX_HORIZON_DEMO' AS horizon", attach_sql)
        self.assertIn("SECRET snowflake_oauth", attach_sql)
        self.assertIn("STAGE_CREATE_TABLES false", attach_sql)
        self.assertIn("DISABLE_MULTI_TABLE_COMMIT true", attach_sql)
        self.assertIn("SKIP_CREATE_TABLE_METADATA_UPDATES true", attach_sql)
        self.assertIn("REMOVE_FILES_ON_DELETE false", attach_sql)

    def test_aws_targets_use_credential_chain_and_endpoint_type(self):
        env = {
            "AWS_ACCESS_KEY_ID": "fake-aws-key",
            "AWS_SECRET_ACCESS_KEY": "fake-aws-secret",
            "AWS_GLUE_REST_ACCOUNT_ID": "123456789012",
            "AWS_GLUE_REST_REGION": "us-west-2",
            "AWS_GLUE_REST_SCHEMA": "irc_duckdb_bench",
            "AWS_GLUE_REST_TABLE_LOCATION_ROOT": "s3://bucket/glue",
            "AWS_S3_TABLES_BUCKET_ARN": "arn:aws:s3tables:us-west-2:123456789012:bucket/demo",
            "AWS_S3_TABLES_REGION": "us-west-2",
        }
        targets = self.bench.load_targets(env)

        glue_secret_sql = self.bench.render_secret_sql(targets["aws_glue"], env)
        glue_attach_sql = self.bench.render_attach_sql(
            targets["aws_glue"], env, self.bench.ATTACH_VARIANTS["no_stage_no_purge"]
        )
        self.assertIn("CREATE OR REPLACE SECRET aws_sigv4", glue_secret_sql)
        self.assertIn("PROVIDER credential_chain", glue_secret_sql)
        self.assertIn("ATTACH '123456789012' AS aws_glue", glue_attach_sql)
        self.assertIn("ENDPOINT_TYPE 'glue'", glue_attach_sql)
        self.assertNotIn("AUTHORIZATION_TYPE", glue_attach_sql)
        self.assertIn("STAGE_CREATE_TABLES false", glue_attach_sql)
        self.assertIn("PURGE_REQUESTED false", glue_attach_sql)

        s3_tables_attach_sql = self.bench.render_attach_sql(
            targets["aws_s3_tables"], env, self.bench.ATTACH_VARIANTS["no_stage_create"]
        )
        self.assertIn(
            "ATTACH 'arn:aws:s3tables:us-west-2:123456789012:bucket/demo' AS aws_s3_tables",
            s3_tables_attach_sql,
        )
        self.assertIn("ENDPOINT_TYPE 's3_tables'", s3_tables_attach_sql)

    def test_glue_workload_create_table_includes_location_property(self):
        env = {
            "AWS_ACCESS_KEY_ID": "fake-aws-key",
            "AWS_SECRET_ACCESS_KEY": "fake-aws-secret",
            "AWS_GLUE_REST_ACCOUNT_ID": "123456789012",
            "AWS_GLUE_REST_TABLE_LOCATION_ROOT": "s3://bucket/glue",
        }
        target = self.bench.load_targets(env)["aws_glue"]
        size = self.bench.BenchmarkSize("tiny", 4)
        sql = self.bench.render_workload_sql(
            target, "no_stage_create", size, repetition=1, keep_tables=False
        )

        self.assertIn("CREATE TABLE aws_glue.irc_duckdb_bench.bench_no_stage_create_tiny_r1", sql)
        self.assertIn(
            "WITH ('location' = 's3://bucket/glue/bench_no_stage_create_tiny_r1/')",
            sql,
        )

    def test_workload_sql_varies_table_name_and_row_count(self):
        target = self.bench.load_targets()["lakekeeper_local"]
        size = self.bench.BenchmarkSize("small", 10_000)
        sql = self.bench.render_workload_sql(
            target, "legacy_full_compat", size, repetition=2, keep_tables=False
        )

        self.assertIn("bench_legacy_full_compat_small_r2", sql)
        self.assertIn("FROM range(10000)", sql)
        self.assertIn("SELECT count(*) AS row_count", sql)
        self.assertIn("DELETE FROM lakekeeper.default.bench_legacy_full_compat_small_r2", sql)
        self.assertIn("WHERE id % 2 = 0", sql)
        self.assertIn("read_after_delete small rep 2", sql)
        self.assertIn("count(*) = 5000", sql)
        self.assertIn("COALESCE(sum(id), 0) = 25000000", sql)
        self.assertIn("error('delete verification failed')", sql)
        self.assertIn(
            "DROP TABLE IF EXISTS lakekeeper.default.bench_legacy_full_compat_small_r2", sql
        )

    def test_tpch_read_workload_materializes_tables_and_runs_read_queries(self):
        target = self.bench.load_targets()["lakekeeper_local"]
        size = self.bench.BenchmarkSize("sf_0_01", 60_012, scale_factor=0.01)
        sql = self.bench.render_workload_sql(
            target,
            "default",
            size,
            repetition=1,
            keep_tables=False,
            workload="tpch-read",
        )

        self.assertIn("INSTALL tpch;", sql)
        self.assertIn("LOAD tpch;", sql)
        self.assertIn("CALL dbgen(sf=0.01);", sql)
        self.assertIn(">>> PHASE: tpch_load sf_0_01 rep 1", sql)
        self.assertIn(
            "CREATE TABLE lakekeeper.default.bench_default_sf_0_01_r1_lineitem AS "
            "SELECT * FROM lineitem;",
            sql,
        )
        self.assertIn(">>> PHASE: tpch_q01 sf_0_01 rep 1", sql)
        self.assertIn(">>> PHASE: tpch_q03 sf_0_01 rep 1", sql)
        self.assertIn(">>> PHASE: tpch_q06 sf_0_01 rep 1", sql)
        self.assertIn(
            "DROP TABLE IF EXISTS lakekeeper.default.bench_default_sf_0_01_r1_lineitem;",
            sql,
        )
        self.assertNotIn("DELETE FROM lakekeeper", sql)

    def test_run_sql_loads_required_extensions_after_disabling_autoload(self):
        env = {
            "LAKEKEEPER_S3_KEY_ID": "fake-lakekeeper-key",
            "LAKEKEEPER_S3_SECRET": "fake-lakekeeper-secret",
        }
        target = self.bench.load_targets(env)["lakekeeper_local"]
        sql, _ = self.bench.render_run_sql(
            target=target,
            env=env,
            variant=self.bench.ATTACH_VARIANTS["default"],
            size=self.bench.BenchmarkSize("tiny", 4),
            repetition=1,
            output_dir=ROOT / ".tmp",
            threads=4,
            memory_limit="4GB",
            keep_tables=False,
        )

        self.assertIn(
            "SET autoload_known_extensions=false;\nLOAD iceberg;\nLOAD httpfs;",
            sql,
        )
        self.assertNotIn("LOAD aws;", sql)

    def test_aws_run_sql_loads_aws_extension_after_disabling_autoload(self):
        env = {
            "AWS_ACCESS_KEY_ID": "fake-aws-key",
            "AWS_SECRET_ACCESS_KEY": "fake-aws-secret",
            "AWS_S3_TABLES_BUCKET_ARN": "arn:aws:s3tables:us-west-2:123456789012:bucket/demo",
        }
        target = self.bench.load_targets(env)["aws_s3_tables"]
        sql, _ = self.bench.render_run_sql(
            target=target,
            env=env,
            variant=self.bench.ATTACH_VARIANTS["no_stage_create"],
            size=self.bench.BenchmarkSize("tiny", 4),
            repetition=1,
            output_dir=ROOT / ".tmp",
            threads=4,
            memory_limit="4GB",
            keep_tables=False,
        )

        self.assertIn(
            "SET autoload_known_extensions=false;\nLOAD iceberg;\nLOAD httpfs;\nLOAD aws;",
            sql,
        )

    def test_redaction_removes_known_secret_values_and_bearer_headers(self):
        env = {
            "HORIZON_ACCESS_TOKEN": "super-secret-token",
            "POLARIS_SECRET": "polaris-secret",
            "POLARIS_ID": "client-id-is-not-secret",
        }
        text = (
            "TOKEN 'super-secret-token'\n"
            "Authorization='Basic YmFkLWJhc2lj'\n"
            "Authorization='AWS4-HMAC-SHA256 "
            "Credential=AKIA/20260624/us-east-1/s3/aws4_request, "
            "SignedHeaders=host, Signature=deadbeef'\n"
            "Authorization=Bearer abc.def\n"
            "client_secret=polaris-secret\n"
            "x-amz-security-token='bad-session-token'\n"
            "x-amz-id-2=response-id-that-looks-like-ASIA-token\n"
            "https://example.com/path?X-Amz-Credential=AKIA%2F20260624&X-Amz-Signature=deadbeef&X-Amz-Security-Token=bad-query-token\n"
            "id=client-id-is-not-secret"
        )

        redacted = self.bench.redact(text, env)

        self.assertNotIn("super-secret-token", redacted)
        self.assertNotIn("polaris-secret", redacted)
        self.assertNotIn("YmFkLWJhc2lj", redacted)
        self.assertNotIn("AWS4-HMAC-SHA256 Credential=AKIA", redacted)
        self.assertNotIn("Bearer abc.def", redacted)
        self.assertNotIn("bad-session-token", redacted)
        self.assertNotIn("response-id-that-looks-like-ASIA-token", redacted)
        self.assertNotIn("deadbeef", redacted)
        self.assertNotIn("bad-query-token", redacted)
        self.assertIn("client-id-is-not-secret", redacted)

    def test_summary_error_redacts_http_debug_secrets(self):
        output = (
            "noise before failure\n"
            "{'request': {'headers': {Authorization='AWS4-HMAC-SHA256 "
            "Credential=AKIA/20260624/us-east-1/s3/aws4_request, "
            "SignedHeaders=host, Signature=deadbeef', "
            "x-amz-security-token='bad-session-token "
            "TransactionContext Error: Failed to commit\n"
        )

        error = self.bench.redacted_error(output, {})

        self.assertNotIn("AKIA", error)
        self.assertNotIn("deadbeef", error)
        self.assertNotIn("bad-session-token", error)
        self.assertNotIn("AWS4-HMAC-SHA256 Credential", error)

    def test_http_debug_output_is_summarized_by_phase_and_url_group(self):
        output = (
            ">>> PHASE: attach\n"
            "\x1b[33m{'request': {'type': GET, "
            "'url': 'https://catalog.example/v1/config?warehouse=demo', "
            "'duration_ms': 12}, 'response': {'status': OK_200}}\x1b[00m\n"
            ">>> PHASE: insert small rep 1\n"
            "{'request': {'type': PUT, "
            "'url': 'https://bucket.s3.us-east-1.amazonaws.com/table/data/file.parquet"
            "?X-Amz-Signature=deadbeef', "
            "'duration_ms': 34}, 'response': {'status': OK_200}}\n"
        )

        events = self.bench.parse_http_debug_output(output)
        self.assertEqual(
            events,
            [
                {
                    "phase": "attach",
                    "method": "GET",
                    "status": "OK_200",
                    "duration_ms": 12,
                    "url_group": "rest_config",
                    "host": "catalog.example",
                    "path": "/v1/config",
                },
                {
                    "phase": "insert small rep 1",
                    "method": "PUT",
                    "status": "OK_200",
                    "duration_ms": 34,
                    "url_group": "object_data",
                    "host": "bucket.s3.us-east-1.amazonaws.com",
                    "path": "/table/data/file.parquet",
                },
            ],
        )

        summary = self.bench.summarize_http_events(events)
        self.assertEqual(summary["http_request_count"], 2)
        self.assertEqual(summary["http_duration_ms"], 46)
        self.assertEqual(summary["http_groups"]["rest_config"], {"count": 1, "duration_ms": 12})
        self.assertEqual(summary["http_groups"]["object_data"], {"count": 1, "duration_ms": 34})
        self.assertEqual(
            summary["http_phase_groups"]["insert small rep 1"]["object_data"],
            {"count": 1, "duration_ms": 34},
        )

    def test_table_urls_are_classified_before_namespace_urls(self):
        self.assertEqual(
            self.bench.classify_url(
                "https://catalog.example/v1/demo/namespaces/ns/tables/table_name"
            ),
            "rest_table_commit_or_load",
        )
        self.assertEqual(
            self.bench.classify_url("https://catalog.example/v1/demo/namespaces/ns/tables"),
            "rest_create_table",
        )


if __name__ == "__main__":
    unittest.main()
