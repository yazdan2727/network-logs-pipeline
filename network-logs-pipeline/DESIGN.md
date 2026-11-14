# Pipeline Design

## 1. Architecture and Design Decisions

The pipeline follows a layered ETL architecture:

**Raw → Validation → Rejected / Staging → Analytics Mart**

The Raw layer preserves the original input values as text. This prevents malformed records from being lost or altered during ingestion and allows validation to be repeated.

Each input file is identified by its SHA-256 hash. The hash acts as the batch identifier and prevents the same file from being ingested more than once.

Validation applies data quality rules to timestamps, IP addresses, ports, byte counts, and duplicate business keys. Invalid records are stored in a separate rejection table with their original values and rejection reasons.

Validated records are converted into appropriate PostgreSQL data types in Staging. Missing byte values are imputed using the batch median, while empty statuses are replaced with `UNKNOWN`.

The Analytics Mart contains daily metrics grouped by UTC event date and source IP. Aggregation is implemented as an upsert so that rerunning a batch does not double-count records.

## 2. Database Design

The database separates ingestion metadata, raw records, rejected records, cleaned records, and analytical aggregates.

Composite primary keys and unique constraints enforce record-level uniqueness. Indexes on batch identifiers, timestamps, source IPs, and rejection reasons support common pipeline operations and analytical queries.

## 3. Scaling to Billions of Records

For significantly larger datasets, the pipeline would require changes in both processing and storage.

- **Partitioning:** Partition large Raw and Staging tables by ingestion date or event date to improve query performance and simplify data retention.
- **Distributed processing:** Replace single-process Python transformations with Apache Spark to distribute validation, deduplication, and aggregation across a cluster.
- **Bulk ingestion:** Use PostgreSQL `COPY` or a bulk-loading mechanism rather than individual inserts.
- **Storage:** Store immutable raw data in object storage such as S3, using columnar formats such as Parquet. A lakehouse table format such as Apache Iceberg could provide schema evolution and reliable incremental processing.
- **Orchestration:** Introduce Airflow to schedule jobs, manage dependencies, support retries, and monitor execution.
- **Monitoring:** Track throughput, processing latency, rejected-record ratios, and batch failures.

## 4. Streaming and Late-Arriving Data

For streaming ingestion, Kafka could receive network events and distribute them to downstream consumers. A streaming processor such as Spark Structured Streaming could validate and aggregate events incrementally.

Event time should be distinguished from ingestion time. Watermarks can define how long the system waits for late-arriving events before finalizing a time window.

When late records arrive after a daily aggregate has been produced, the affected event-date and source-IP combinations should be recalculated or incrementally updated. Idempotent writes and event identifiers are necessary to prevent duplicate counting during retries.

## 5. Trade-offs

The current implementation prioritizes correctness, traceability, and simplicity over distributed processing. PostgreSQL and Python are appropriate for the assignment's dataset size and make the transformation logic easy to inspect and test.

The architecture preserves clear boundaries between ingestion, validation, cleaning, and aggregation, allowing individual components to be replaced as data volume and operational requirements grow.