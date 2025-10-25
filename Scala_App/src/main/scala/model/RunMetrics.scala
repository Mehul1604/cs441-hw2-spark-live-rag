package model

final case class RunMetrics(
  run_id: String, run_ts: Long,
  docs_scanned: Long, bytes_scanned: Long,
  docs_new_or_changed: Long, chunks_new_or_changed: Long,
  embeddings_new: Long, embeddings_reused: Long,
  t_read_ms: Long, t_delta_ms: Long, t_chunk_ms: Long, t_embed_ms: Long, t_publish_ms: Long, t_total_ms: Long,
  docs_per_sec: Double, embeddings_per_sec: Double,
  docs_skipped_ratio: Double, embeddings_reuse_ratio: Double,
  max_source_mtime: Long, publish_ts: Long, freshness_ms: Long,
  snapshot_path: String, model_version: String, dim: Int, num_shards: Int,
  noop_run: Boolean
)
