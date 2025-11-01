# Incremental RAG Delta Indexer (CS-441 HW2)

A Spark-based pipeline that incrementally indexes a corpus of PDFs for Retrieval-Augmented Generation (RAG). It extracts text, detects language, chunks content, generates embeddings via Ollama, stores data in Delta Lake tables, and publishes versioned retrieval index snapshots. Designed to run locally, against HDFS/S3 via Spark submit, and on AWS EMR.


## What this project does
- Reads PDFs from a configured location (local, HDFS, or S3)
- Extracts and normalizes text (robust PDF parsing with PDFBox/Tika)
- Detects language (OpenNLP-based utility)
- Chunks text with overlap and optional smart boundaries
- Generates vector embeddings via Ollama (defaults to mxbai-embed-large)
- Upserts documents, chunks, and embeddings into Delta tables (rag.*)
- Publishes a versioned retrieval index snapshot and writes it to configured storage
- Records run metrics (counts, timings, reuse ratios) in Delta

## EMR video walkthrough
- Add your EMR demo/walkthrough link here (e.g., YouTube):
  - https://example.com/your-emr-video


## Repository layout
Top-level
- `Scala_App/` – main Scala project (sbt)
- `data/` – sample corpus and local snapshot folders
- `emr-bootstrap.sh` – bootstrap script to prep an EMR cluster (Java, Ollama, APP_ENV)
- `README.md` – this file

Scala project (Scala_App)
- `build.sbt` – builds Local (Scala 2.13, Spark 4.0.1) or EMR (Scala 2.12, Spark 3.5.5) based on a flag
- `src/main/scala/`
  - `DeltaIndexerDriver.scala` – main entry point (pass flags like --env, --reindex)
  - `config/AppConfig.scala` – Typesafe config loader with environment overlays
  - `llm/`
    - `OllamaClient.scala` – chat + embeddings using openai-scala client to talk to Ollama
    - `SerializableEmbeddingGenerator.scala` – Spark-safe embedding wrapper
    - `Embeddings.scala`, `EmbeddingLimiter.scala`, `PerExecutor.scala` – helpers for rate/exec control
  - `utils/`
    - `PdfTextExtractor.scala` – text extraction; safeExtractText supports local, hdfs:// and s3://
    - `LanguageDetector.scala` – serializable detector + sampling util
    - `PdfChunker` (object inside `PdfTextExtractor.scala`) – chunking utilities
  - `model/` – small case classes (ChunkData, ExtractTextResult, RunMetrics)
- `src/main/resources/`
  - `application.conf` – main configuration and environment overrides (local, emr)
  - `logback.xml` – logging
- `src/test/scala/`
  - `llm/` – `OllamaClientSpec.scala`, `SerializableEmbeddingGeneratorSpec.scala`
  - `utils/` – `PdfTextExtractorSpec.scala`, `LanguageDetectorSpec.scala`
  - `config/` – `AppConfigSpec.scala`
- `metastore_db/`, `spark-warehouse/` – local Hive/Delta storage when running with Hive support


## Building
Prereqs
- Java 17 (OpenJDK or Corretto)
- sbt 1.9+
- Optional: Local Spark if you prefer spark-submit (the Local fat JAR works without it)
- Ollama running locally or accessible remotely for embeddings (see OLLAMA_HOST)

Build JARs
- Local (Scala 2.13 + Spark 4.0.1 in JAR)
  - `sbt clean assembly`
  - Produces `target/scala-2.13/rag-delta-indexer-local-assembly.jar`
- EMR (Scala 2.12 + Spark 3.5.5 provided by EMR)
  - `sbt -Demr=true clean assembly`
  - Produces `target/scala-2.12/rag-delta-indexer-emr-assembly.jar`


## Running modes
Flags supported by the app
- `--env=<local|emr>` – selects config overlay from application.conf (default: local)
- `--reindex` – full rebuild of tables (drop/create); otherwise incremental
- `--show-tables` – list databases/tables and exit
- `--model=<name>` – override embedding model name at runtime (default from config is mxbai-embed-large)

### A) Local development (sbt runMain)
Run fully from sbt using the config under `environments.local`:
```bash
cd Scala_App
sbt "runMain DeltaIndexerDriver --env=local --model=mxbai-embed-large"
```
Common variations:
- Incremental (default): omit `--reindex`
- Full rebuild: add `--reindex`
- Inspect catalog: add `--show-tables`

Ensure Ollama is reachable. By default the code auto-detects Ollama, or set:
```bash
export OLLAMA_HOST=http://localhost:11434
```

### B) Local machine with spark-submit (using fat JAR)
1) Build the JAR:
```bash
cd Scala_App
sbt clean assembly
```
2) Submit locally with Spark (example provided by assignment):
```bash
spark-submit \
  --master "local[*]" \
  --class DeltaIndexerDriver \
  --driver-java-options "-Dhadoop.home.dir=/opt/homebrew/opt/hadoop/ -Dhadoop.conf.dir=/opt/homebrew/opt/hadoop/libexec/etc/hadoop/ -Djava.net.preferIPv4Stack=true" \
  --packages io.delta:delta-core_2.13:2.4.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.sql.warehouse.dir=/spark/warehouse" \
  target/scala-2.13/rag-delta-indexer-local-assembly.jar \
  --env=local --model=mxbai-embed-large
```
Notes:
- Because the local assembly includes Spark/Delta deps, the `--packages` line may be optional; keeping it is fine.
- If you intend to read from HDFS paths, make sure your Hadoop config/env is on the classpath (example driver-java-options above).

### C) EMR + S3 (YARN)
1) Build the EMR-compatible JAR (Scala 2.12, Spark provided by EMR 7.10 / Spark 3.5.5):
```bash
cd Scala_App
sbt -Demr=true clean assembly
```
2) Copy the JAR to S3 or the EMR master node.
3) On EMR, submit with YARN and the EMR environment:
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class DeltaIndexerDriver \
  target/scala-2.12/rag-delta-indexer-emr-assembly.jar \
  --env=emr --model=mxbai-embed-large
```
- The bootstrap script `emr-bootstrap.sh` also sets `APP_ENV=emr` on the nodes. The app selects the env from `--env=...`, so keep the flag. The APP_ENV is helpful for shell tooling and consistency.
- application.conf maps `environments.emr` to S3 URIs for `data.source.folder`, `spark.warehouse.dir`, and `data.snapshots.base.path`.
- Ensure AWS permissions for S3 and Ollama availability (install via bootstrap; the script pulls mxbai-embed-large and warms it up).

## End-to-end flow and incremental logic
This job is designed to be idempotent and incremental. High-level steps:
1) Discover PDFs
- Read all PDF files from `data.source.folder` using Spark binaryFile.
2) Extract + normalize text
- For each file, `safeExtractText` returns text + debug info (supports local, hdfs://, s3://).
3) Detect language
- Serializable detector runs on executors (with optional sampling for large text).
4) Chunk text
- Overlapping windows with optional intelligent cuts; chunk indices and ranges are preserved.
5) Generate embeddings
- Batching and retry logic via `OllamaClient` and `SerializableEmbeddingGenerator`.
6) Upsert tables and publish index snapshot
- MERGE into rag tables; create a versioned snapshot and write to configured path.

How delta detection works
- Documents: compute `docId = sha2(pdfUri)` and `contentHash = sha2(text)`.
  - New/changed docs are those NOT matching existing `(docId, contentHash)`.
- Chunks: operate only on docs selected above; chunk rows carry `chunkId = sha2(docId:chunkIndex:chunkStart:chunkEnd)` and `chunkContentHash = sha2(chunkText)`.
- Embeddings: recompute only for chunks with new `(chunkId, chunkContentHash)` for the selected `(modelName, modelVersion)`.

Concrete scenarios
- Initial build: “Add 10 PDFs with --reindex”
  - The job drops and recreates the rag schema. All 10 PDFs are extracted, chunked, embedded, and all three tables are created from scratch. A snapshot table `rag.retrieval_index_<ts>` is created and the dataset is written to `data.snapshots.base.path/<model>/<version>/<ts>`.
- Incremental add: “Add 2 more PDFs later”
  - The job reads all 12 files but determines delta by `(docId, contentHash)`. Only the 2 new PDFs are considered “new/changed”. Only their chunks are generated and only those chunks are embedded. Existing 10 remain untouched.
- Content edit in place: “Edit a PDF at the same path”
  - `docId = sha2(pdfUri)` remains the same; `contentHash = sha2(text)` changes. The doc becomes “changed”. New chunks are produced; for embeddings, only chunks whose `chunkContentHash` changed are re-embedded. Unchanged chunks are reused.

Why this works
- Doc-level change detection ensures we only reprocess impacted docs.
- Chunk-level hashing ensures identical chunk text is never re-embedded.
- Embeddings keyed by `(chunkId, modelName, modelVersion)` ensure per-model reproducibility and safe updates when text changes.


## Delta table schemas, keys, and merge logic
The job writes both staging and final Delta tables, then uses SQL MERGE to upsert.

Tables created/maintained
- rag.delta_pdf_docs
  - Columns (representative):
    - `docId` STRING – sha2(pdfUri)
    - `pdfUri` STRING – original input URI/path
    - `title` STRING – first line of text
    - `language` STRING
    - `text` STRING – full extracted text
    - `contentHash` STRING – sha2(text)
    - `debugInfo` STRING – extractor diagnostics
  - Key and merge:
    - MERGE ON `target.docId = source.docId`
    - When matched: UPDATE SET *; when not matched: INSERT *

- rag.delta_pdf_chunks
  - Columns:
    - `chunkId` STRING – sha2(docId:chunkIndex:chunkStartIndex:chunkEndIndex)
    - `docId` STRING
    - `chunkIndex` INT
    - `chunkStartIndex` INT
    - `chunkEndIndex` INT
    - `chunkText` STRING
    - `sectionPath` STRING – original pdfUri
    - `chunkContentHash` STRING – sha2(chunkText)
  - Key and merge:
    - MERGE ON `target.chunkId = source.chunkId`
    - When matched: UPDATE SET *; when not matched: INSERT *

- rag.delta_embeddings
  - Columns:
    - `chunkId` STRING
    - `chunkContentHash` STRING
    - `modelName` STRING
    - `modelVersion` STRING
    - `embedding` ARRAY<FLOAT> (vector)
  - Key and merge:
    - MERGE ON `target.chunkId = source.chunkId AND target.modelName = source.modelName AND target.modelVersion = source.modelVersion`
    - When matched: UPDATE SET *; when not matched: INSERT *
  - Delta selection before embedding:
    - Existing subset computed as `(chunkId, chunkContentHash)` for the given `(modelName, modelVersion)`.
    - Only left_anti rows are embedded, so unchanged chunks are reused.

- rag.run_metrics
  - Columns (abbrev.): counts, timings, rates, ratios, snapshot_path, model_version, noop_run flag.

Staging tables (physicalized to break lineage and speed MERGE)
- `rag.staging_pdf_docs_temp`
- `rag.staging_pdf_chunks_temp`
- `rag.staging_pdf_embeddings_temp`

Snapshot creation
- A versioned table `rag.retrieval_index_<ts>` is created as:
  - `SELECT c.chunkId, c.docId, c.chunkText, c.sectionPath, d.title, d.language, e.embedding, e.modelName as embedder, e.modelVersion as embedder_ver, c.chunkContentHash as contentHash, current_timestamp() as version_ts`
  - `FROM rag.delta_pdf_chunks c JOIN rag.delta_pdf_docs d USING(docId) JOIN rag.delta_embeddings e USING(chunkId)`
- The same dataset is also written to `${data.snapshots.base.path}/${modelName}/${modelVersion}/<ts>` as Delta.

Notes on keys and updates
- Doc edits at same URI update the doc row (same docId) and ripple through chunks/embeddings where text changed.
- If chunk segmentation stays identical (same index/start/end), `chunkId` is stable; if `chunkText` changes, `chunkContentHash` changes and re-embedding is triggered.
- Embeddings are model/version-specific, enabling side-by-side indices for different models.

## Configuration (Typesafe config)
Main file: `Scala_App/src/main/resources/application.conf`
- app.name, app.version
- spark
  - `master` (default local[*], emr uses yarn)
  - `warehouse.dir` (local: `/spark/warehouse`, emr: `s3://.../warehouse/`)
  - `extensions`, `catalog` (Delta Lake wiring)
- data
  - `source.folder` – where PDFs come from (local path, hdfs://, or s3://)
  - `downloaded.pdfs.dir` – temp staging for extraction
  - `snapshots.base.path` – where versioned index snapshots are written
- embedding
  - model.name (default `mxbai-embed-large`) and model.version
  - batch.size (embedding calls in partitions)
  - ollama.host, ollama.port, timeout.seconds
- environments
  - `local` – defaults to POSIX paths under `/spark/...`
  - `emr` – uses S3 paths and YARN

Important notes
- In the provided config, `environments.local` points to `/spark/...`. To run fully local with your own folders, adjust these paths to your machine and run with `--env=local`.
- `safeExtractText` supports `file:`, plain local paths, `hdfs://` and `s3://` URIs. For HDFS/S3, Hadoop configuration must be present on the classpath or supplied via JVM opts.
- Embedding model name can be overridden at runtime with `--model=<name>`; otherwise it uses `embedding.model.name`.
- Ollama host detection prefers `$OLLAMA_HOST` if set; otherwise it attempts local/EC2 metadata detection.


## Hive support, metastore, and Delta
- The app enables Hive support: `SparkSession.builder.enableHiveSupport()`
- Locally, Spark will use a Derby metastore under `Scala_App/metastore_db/` and warehouse at `Scala_App/spark-warehouse/` (or as configured).
- On EMR, the warehouse points to S3; EMR provides Hive/Spark runtime. Delta tables live under the configured warehouse and table locations.

Delta tables created/used
- `rag.delta_pdf_docs` – document-level extracted metadata
- `rag.delta_pdf_chunks` – chunked text
- `rag.delta_embeddings` – embeddings keyed by chunkId + model
- `rag.run_metrics` – per-run metrics
- Versioned snapshot: `rag.retrieval_index_<ts>` and a Delta dataset written to `data.snapshots.base.path/model/version/<ts>`


## Components overview
- Driver: `DeltaIndexerDriver`
  - Parses flags; loads `AppConfig` based on `--env`
  - Reads PDFs, extracts text (`safeExtractText`), detects language, chunks text, embeds with `SerializableEmbeddingGenerator` + `OllamaClient`, writes/merges Delta tables, publishes snapshot, logs metrics
- LLM layer
  - `OllamaClient`: embeddings + chat via openai-scala client pointed at Ollama; batching & retries
  - `SerializableEmbeddingGenerator`: Spark-serializable wrapper that uses a per-executor client
- Utils
  - `PdfTextExtractor`: robust PDF extraction; `safeExtractText` supports local/HDFS/S3
  - `LanguageDetector`: serializable detection with sampling helpers
  - `PdfChunker`: windowed, overlapping chunking with optional intelligent cuts
- Config
  - `AppConfig`: case classes + Typesafe loader; environment overlay resolution
- Models
  - `ChunkData`, `ExtractTextResult`, `RunMetrics`


## Testing
Tests are written with ScalaTest and Mockito where appropriate.
- Run all tests:
```bash
cd Scala_App
sbt test
```
- Areas covered:
  - llm: `OllamaClientSpec`, `SerializableEmbeddingGeneratorSpec`
  - utils: `PdfTextExtractorSpec`, `LanguageDetectorSpec`
  - config: `AppConfigSpec`


## Quick examples
- Local incremental run with defaults:
```bash
cd Scala_App
sbt "runMain DeltaIndexerDriver --env=local"
```
- Local full reindex and custom model:
```bash
cd Scala_App
sbt "runMain DeltaIndexerDriver --env=local --reindex --model=mxbai-embed-large"
```
- EMR cluster run (after assembling EMR jar):
```bash
spark-submit --master yarn --deploy-mode cluster \
  --class DeltaIndexerDriver \
  target/scala-2.12/rag-delta-indexer-emr-assembly.jar \
  --env=emr --model=mxbai-embed-large
```


## Troubleshooting
- ClassNotFoundException (main class): ensure `--class DeltaIndexerDriver` and the correct jar are used
- Using local config on EMR accidentally: pass `--env=emr`
- Ollama connectivity: set `export OLLAMA_HOST=http://<host>:11434` if not on localhost
- S3/HDFS read issues: ensure Hadoop/Spark have correct configs/creds; use `-Dhadoop.conf.dir=...` if submitting locally

---
Happy indexing!
