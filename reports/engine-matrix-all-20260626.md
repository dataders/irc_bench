# Engine matrix combined report

Rows: 48 benchmark result rows.
Parquet: `reports/engine-matrix-all-20260626.parquet`

Included run roots:

- `.tmp/catalog_benchmarks/engine-matrix-20260626T2335Z`
- `.tmp/catalog_benchmarks/engine-matrix-large-20260626T233637Z`
- `.tmp/catalog_benchmarks/engine-matrix-medium-20260626T233151Z`

Important comparison note: DuckDB uses the existing CRUD workload, while PyIceberg and Spark use create-write-read. The `operation_s` column excludes engine startup/setup phases where possible, but DuckDB still includes delete and read-after-delete work.

## Row counts by size

| size | benchmark_rows |
| --- | --- |
| large | 12 |
| medium | 12 |
| small | 12 |
| tiny | 12 |

## Fastest operation by size and catalog

| size | catalog | fastest | slowest | spread |
| --- | --- | --- | --- | --- |
| large | aws_glue | spark 30.975s | duckdb 41.912s | 1.35x |
| large | aws_s3_tables | pyiceberg 18.060s | duckdb 39.609s | 2.19x |
| large | horizon | pyiceberg 25.497s | duckdb 32.138s | 1.26x |
| large | polaris_remote | spark 8.437s | duckdb 20.061s | 2.38x |
| medium | aws_glue | spark 9.340s | pyiceberg 9.740s | 1.04x |
| medium | aws_s3_tables | pyiceberg 5.479s | spark 12.513s | 2.28x |
| medium | horizon | pyiceberg 12.693s | duckdb 24.979s | 1.97x |
| medium | polaris_remote | pyiceberg 2.794s | spark 6.160s | 2.20x |
| small | aws_glue | spark 5.474s | pyiceberg 7.950s | 1.45x |
| small | aws_s3_tables | pyiceberg 3.574s | duckdb 7.329s | 2.05x |
| small | horizon | spark 15.228s | duckdb 25.174s | 1.65x |
| small | polaris_remote | pyiceberg 1.564s | spark 2.945s | 1.88x |
| tiny | aws_glue | duckdb 6.286s | spark 9.034s | 1.44x |
| tiny | aws_s3_tables | pyiceberg 3.539s | spark 10.744s | 3.04x |
| tiny | horizon | spark 23.312s | duckdb 45.714s | 1.96x |
| tiny | polaris_remote | pyiceberg 1.777s | spark 5.270s | 2.97x |

## All benchmark results

| size | rows | catalog | engine | workload | variant | passed | total_s | operation_s | read_s | http_ms | requests |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| tiny | 4 | aws_glue | duckdb | crud | no_stage_no_purge | yes | 7.377s | 6.286s | 1.902s | 7249 | 28 |
| tiny | 4 | aws_glue | pyiceberg | create-write-read | default | yes | 7.946s | 7.740s | 1.302s | 0 | 0 |
| tiny | 4 | aws_glue | spark | create-write-read | default | yes | 15.142s | 9.034s | 1.741s | 0 | 0 |
| tiny | 4 | aws_s3_tables | duckdb | crud | no_stage_create | yes | 7.406s | 6.415s | 1.847s | 7292 | 29 |
| tiny | 4 | aws_s3_tables | pyiceberg | create-write-read | default | yes | 3.964s | 3.539s | 0.708s | 0 | 0 |
| tiny | 4 | aws_s3_tables | spark | create-write-read | default | yes | 14.550s | 10.744s | 1.727s | 0 | 0 |
| tiny | 4 | horizon | duckdb | crud | stage_multi_metadata | yes | 48.874s | 45.714s | 4.539s | 48780 | 28 |
| tiny | 4 | horizon | pyiceberg | create-write-read | default | yes | 30.113s | 29.480s | 3.958s | 0 | 0 |
| tiny | 4 | horizon | spark | create-write-read | default | yes | 28.211s | 23.312s | 1.400s | 0 | 0 |
| tiny | 4 | polaris_remote | duckdb | crud | default | yes | 2.873s | 2.389s | 0.657s | 2723 | 30 |
| tiny | 4 | polaris_remote | pyiceberg | create-write-read | default | yes | 2.043s | 1.777s | 0.292s | 0 | 0 |
| tiny | 4 | polaris_remote | spark | create-write-read | default | yes | 21.902s | 5.270s | 1.033s | 0 | 0 |
| small | 10000 | aws_glue | duckdb | crud | no_stage_no_purge | yes | 8.004s | 7.094s | 2.478s | 7875 | 30 |
| small | 10000 | aws_glue | pyiceberg | create-write-read | default | yes | 8.051s | 7.950s | 1.551s | 0 | 0 |
| small | 10000 | aws_glue | spark | create-write-read | default | yes | 7.647s | 5.474s | 1.279s | 0 | 0 |
| small | 10000 | aws_s3_tables | duckdb | crud | no_stage_create | yes | 8.304s | 7.329s | 2.339s | 8190 | 31 |
| small | 10000 | aws_s3_tables | pyiceberg | create-write-read | default | yes | 3.878s | 3.574s | 0.964s | 0 | 0 |
| small | 10000 | aws_s3_tables | spark | create-write-read | default | yes | 8.110s | 6.122s | 1.387s | 0 | 0 |
| small | 10000 | horizon | duckdb | crud | stage_multi_metadata | yes | 28.289s | 25.174s | 3.795s | 28207 | 29 |
| small | 10000 | horizon | pyiceberg | create-write-read | default | yes | 23.307s | 22.367s | 2.173s | 0 | 0 |
| small | 10000 | horizon | spark | create-write-read | default | yes | 18.728s | 15.228s | 0.690s | 0 | 0 |
| small | 10000 | polaris_remote | duckdb | crud | default | yes | 2.903s | 2.407s | 0.713s | 2820 | 32 |
| small | 10000 | polaris_remote | pyiceberg | create-write-read | default | yes | 1.716s | 1.564s | 0.441s | 0 | 0 |
| small | 10000 | polaris_remote | spark | create-write-read | default | yes | 6.384s | 2.945s | 0.448s | 0 | 0 |
| medium | 1000000 | aws_glue | duckdb | crud | no_stage_no_purge | yes | 10.729s | 9.672s | 3.648s | 11988 | 41 |
| medium | 1000000 | aws_glue | pyiceberg | create-write-read | default | yes | 9.893s | 9.740s | 2.388s | 0 | 0 |
| medium | 1000000 | aws_glue | spark | create-write-read | default | yes | 12.165s | 9.340s | 2.293s | 0 | 0 |
| medium | 1000000 | aws_s3_tables | duckdb | crud | no_stage_create | yes | 10.725s | 9.702s | 3.630s | 12195 | 42 |
| medium | 1000000 | aws_s3_tables | pyiceberg | create-write-read | default | yes | 5.913s | 5.479s | 1.611s | 0 | 0 |
| medium | 1000000 | aws_s3_tables | spark | create-write-read | default | yes | 15.354s | 12.513s | 2.374s | 0 | 0 |
| medium | 1000000 | horizon | duckdb | crud | stage_multi_metadata | yes | 27.299s | 24.979s | 3.145s | 27375 | 41 |
| medium | 1000000 | horizon | pyiceberg | create-write-read | default | yes | 13.281s | 12.693s | 1.681s | 0 | 0 |
| medium | 1000000 | horizon | spark | create-write-read | default | yes | 22.459s | 19.453s | 2.174s | 0 | 0 |
| medium | 1000000 | polaris_remote | duckdb | crud | default | yes | 4.637s | 4.146s | 1.247s | 4635 | 43 |
| medium | 1000000 | polaris_remote | pyiceberg | create-write-read | default | yes | 3.049s | 2.794s | 0.790s | 0 | 0 |
| medium | 1000000 | polaris_remote | spark | create-write-read | default | yes | 9.909s | 6.160s | 0.909s | 0 | 0 |
| large | 10000000 | aws_glue | duckdb | crud | no_stage_no_purge | yes | 43.101s | 41.912s | 20.936s | 54006 | 164 |
| large | 10000000 | aws_glue | pyiceberg | create-write-read | default | yes | 33.815s | 33.659s | 10.319s | 0 | 0 |
| large | 10000000 | aws_glue | spark | create-write-read | default | yes | 45.450s | 30.975s | 3.290s | 0 | 0 |
| large | 10000000 | aws_s3_tables | duckdb | crud | no_stage_create | yes | 40.985s | 39.609s | 16.925s | 52770 | 165 |
| large | 10000000 | aws_s3_tables | pyiceberg | create-write-read | default | yes | 18.469s | 18.060s | 5.828s | 0 | 0 |
| large | 10000000 | aws_s3_tables | spark | create-write-read | default | yes | 40.157s | 24.156s | 3.019s | 0 | 0 |
| large | 10000000 | horizon | duckdb | crud | stage_multi_metadata | yes | 35.068s | 32.138s | 7.498s | 35527 | 163 |
| large | 10000000 | horizon | pyiceberg | create-write-read | default | yes | 26.193s | 25.497s | 3.493s | 0 | 0 |
| large | 10000000 | horizon | spark | create-write-read | default | yes | 35.186s | 31.530s | 2.555s | 0 | 0 |
| large | 10000000 | polaris_remote | duckdb | crud | default | yes | 20.608s | 20.061s | 8.364s | 22419 | 166 |
| large | 10000000 | polaris_remote | pyiceberg | create-write-read | default | yes | 10.705s | 10.439s | 2.749s | 0 | 0 |
| large | 10000000 | polaris_remote | spark | create-write-read | default | yes | 13.388s | 8.437s | 1.158s | 0 | 0 |

## Parquet columns

The Parquet file includes the visible report columns plus source metadata, phase timing columns, raw timing JSON, HTTP group JSON, read metric JSON, and the raw summary row JSON.
