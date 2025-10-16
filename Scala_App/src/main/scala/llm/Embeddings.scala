package llm

// Trait for generating text embeddings
trait Embeddings {
  def embed(texts: Vector[String], model: String, batching: Boolean = true): Vector[Vector[Float]]

  def dim(model: String) = 1024
}
