# DuckDB Iceberg Engine Matrix

GitHub-native summary of the combined engine matrix report. Charts live in [the generated HTML](engine-matrix-all-20260626-mviz.html); mviz source is in [`reports/engine-matrix-all-20260626.mviz`](engine-matrix-all-20260626.mviz).

DuckDB uses the CRUD workload. PyIceberg and Spark use create-write-read. `operation_s` excludes engine startup/setup where possible, but DuckDB still includes delete and read-after-delete work.

## Source Files

| Artifact | Path |
| --- | --- |
| Parquet input | `reports/engine-matrix-all-20260626.parquet` |
| Flat CSV export | `reports/engine-matrix-all-20260626.csv` |
| mviz source | `reports/engine-matrix-all-20260626.mviz` |
| Rendered HTML | `reports/engine-matrix-all-20260626-mviz.html` |
| Generated mviz data | `reports/mviz-data/engine-matrix-all-20260626` |

## Fastest Operation By Size And Catalog

| Size | Catalog | Fastest | Slowest | Spread |
| --- | --- | --- | --- | --- |
| tiny | AWS Glue | DuckDB 6.286s | Spark 9.034s | 1.44x |
| tiny | AWS S3 Tables | PyIceberg 3.539s | Spark 10.744s | 3.04x |
| tiny | Snowflake Horizon | Spark 23.312s | DuckDB 45.714s | 1.96x |
| tiny | Polaris remote | PyIceberg 1.777s | Spark 5.270s | 2.97x |
| small | AWS Glue | Spark 5.474s | PyIceberg 7.950s | 1.45x |
| small | AWS S3 Tables | PyIceberg 3.574s | DuckDB 7.329s | 2.05x |
| small | Snowflake Horizon | Spark 15.228s | DuckDB 25.174s | 1.65x |
| small | Polaris remote | PyIceberg 1.564s | Spark 2.945s | 1.88x |
| medium | AWS Glue | Spark 9.340s | PyIceberg 9.740s | 1.04x |
| medium | AWS S3 Tables | PyIceberg 5.479s | Spark 12.513s | 2.28x |
| medium | Snowflake Horizon | PyIceberg 12.693s | DuckDB 24.979s | 1.97x |
| medium | Polaris remote | PyIceberg 2.794s | Spark 6.160s | 2.20x |
| large | AWS Glue | Spark 30.975s | DuckDB 41.912s | 1.35x |
| large | AWS S3 Tables | PyIceberg 18.060s | DuckDB 39.609s | 2.19x |
| large | Snowflake Horizon | PyIceberg 25.497s | DuckDB 32.138s | 1.26x |
| large | Polaris remote | Spark 8.437s | DuckDB 20.061s | 2.38x |

## Operation Seconds By Catalog

| Size | Catalog | DuckDB | PyIceberg | Spark |
| --- | --- | --- | --- | --- |
| tiny | AWS Glue | 6.286s | 7.740s | 9.034s |
| tiny | AWS S3 Tables | 6.415s | 3.539s | 10.744s |
| tiny | Snowflake Horizon | 45.714s | 29.480s | 23.312s |
| tiny | Polaris remote | 2.389s | 1.777s | 5.270s |
| small | AWS Glue | 7.094s | 7.950s | 5.474s |
| small | AWS S3 Tables | 7.329s | 3.574s | 6.122s |
| small | Snowflake Horizon | 25.174s | 22.367s | 15.228s |
| small | Polaris remote | 2.407s | 1.564s | 2.945s |
| medium | AWS Glue | 9.672s | 9.740s | 9.340s |
| medium | AWS S3 Tables | 9.702s | 5.479s | 12.513s |
| medium | Snowflake Horizon | 24.979s | 12.693s | 19.453s |
| medium | Polaris remote | 4.146s | 2.794s | 6.160s |
| large | AWS Glue | 41.912s | 33.659s | 30.975s |
| large | AWS S3 Tables | 39.609s | 18.060s | 24.156s |
| large | Snowflake Horizon | 32.138s | 25.497s | 31.530s |
| large | Polaris remote | 20.061s | 10.439s | 8.437s |

## Operation Seconds By Query Engine

| Size | Engine | AWS Glue | AWS S3 Tables | Snowflake Horizon | Polaris remote |
| --- | --- | --- | --- | --- | --- |
| tiny | DuckDB | 6.286s | 6.415s | 45.714s | 2.389s |
| tiny | PyIceberg | 7.740s | 3.539s | 29.480s | 1.777s |
| tiny | Spark | 9.034s | 10.744s | 23.312s | 5.270s |
| small | DuckDB | 7.094s | 7.329s | 25.174s | 2.407s |
| small | PyIceberg | 7.950s | 3.574s | 22.367s | 1.564s |
| small | Spark | 5.474s | 6.122s | 15.228s | 2.945s |
| medium | DuckDB | 9.672s | 9.702s | 24.979s | 4.146s |
| medium | PyIceberg | 9.740s | 5.479s | 12.693s | 2.794s |
| medium | Spark | 9.340s | 12.513s | 19.453s | 6.160s |
| large | DuckDB | 41.912s | 39.609s | 32.138s | 20.061s |
| large | PyIceberg | 33.659s | 18.060s | 25.497s | 10.439s |
| large | Spark | 30.975s | 24.156s | 31.530s | 8.437s |

## DuckDB HTTP Timings

HTTP debug timings are populated for DuckDB CLI rows only. `Summed HTTP` is summed request duration, not wall time.

| Size | Catalog | Total | Operation | Summed HTTP | Requests |
| --- | --- | --- | --- | --- | --- |
| tiny | AWS Glue | 7.377s | 6.286s | 7.249s | 28 |
| tiny | AWS S3 Tables | 7.406s | 6.415s | 7.292s | 29 |
| tiny | Snowflake Horizon | 48.874s | 45.714s | 48.780s | 28 |
| tiny | Polaris remote | 2.873s | 2.389s | 2.723s | 30 |
| small | AWS Glue | 8.004s | 7.094s | 7.875s | 30 |
| small | AWS S3 Tables | 8.304s | 7.329s | 8.190s | 31 |
| small | Snowflake Horizon | 28.289s | 25.174s | 28.207s | 29 |
| small | Polaris remote | 2.903s | 2.407s | 2.820s | 32 |
| medium | AWS Glue | 10.729s | 9.672s | 11.988s | 41 |
| medium | AWS S3 Tables | 10.725s | 9.702s | 12.195s | 42 |
| medium | Snowflake Horizon | 27.299s | 24.979s | 27.375s | 41 |
| medium | Polaris remote | 4.637s | 4.146s | 4.635s | 43 |
| large | AWS Glue | 43.101s | 41.912s | 54.006s | 164 |
| large | AWS S3 Tables | 40.985s | 39.609s | 52.770s | 165 |
| large | Snowflake Horizon | 35.068s | 32.138s | 35.527s | 163 |
| large | Polaris remote | 20.608s | 20.061s | 22.419s | 166 |

## Remote Catalog Comparison

| Size | Engine | Fastest | Polaris remote | Snowflake Horizon | AWS S3 Tables | Horizon / Polaris | S3 Tables / Polaris |
| --- | --- | --- | --- | --- | --- | --- | --- |
| tiny | DuckDB | Polaris remote | 2.389s | 45.714s | 6.415s | 19.14x | 2.69x |
| tiny | PyIceberg | Polaris remote | 1.777s | 29.480s | 3.539s | 16.59x | 1.99x |
| tiny | Spark | Polaris remote | 5.270s | 23.312s | 10.744s | 4.42x | 2.04x |
| small | DuckDB | Polaris remote | 2.407s | 25.174s | 7.329s | 10.46x | 3.04x |
| small | PyIceberg | Polaris remote | 1.564s | 22.367s | 3.574s | 14.30x | 2.29x |
| small | Spark | Polaris remote | 2.945s | 15.228s | 6.122s | 5.17x | 2.08x |
| medium | DuckDB | Polaris remote | 4.146s | 24.979s | 9.702s | 6.02x | 2.34x |
| medium | PyIceberg | Polaris remote | 2.794s | 12.693s | 5.479s | 4.54x | 1.96x |
| medium | Spark | Polaris remote | 6.160s | 19.453s | 12.513s | 3.16x | 2.03x |
| large | DuckDB | Polaris remote | 20.061s | 32.138s | 39.609s | 1.60x | 1.97x |
| large | PyIceberg | Polaris remote | 10.439s | 25.497s | 18.060s | 2.44x | 1.73x |
| large | Spark | Polaris remote | 8.437s | 31.530s | 24.156s | 3.74x | 2.86x |
