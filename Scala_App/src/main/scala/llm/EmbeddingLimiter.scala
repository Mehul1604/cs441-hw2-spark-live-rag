package llm

import java.util.concurrent.Semaphore

/** Per-executor concurrency limiter for embedding calls. */
object EmbeddingLimiter extends Serializable {
  // 1 permit = fully sequential; change to >1 if you later want controlled parallelism
  private val sem = new Semaphore(1, /*fair=*/true)

  def runOne[T](f: => T): T = {
    sem.acquire()
    try f
    finally sem.release()
  }
}
