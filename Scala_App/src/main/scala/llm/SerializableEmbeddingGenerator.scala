package llm

import org.slf4j.LoggerFactory
import java.io.Serializable

// Serializable wrapper for embedding functionality that can be safely used in Spark tasks
//class SerializableEmbeddingGenerator(
//  modelName: String,
//  modelVersion: String,
//  baseUrl: String = OllamaClient.detectOllamaHost()
//) extends Serializable {
//
//  @transient private lazy val logger = LoggerFactory.getLogger(this.getClass)
//
//  def generateEmbeddings(texts: Vector[String]): Vector[Vector[Float]] = {
//    if (texts.isEmpty) return Vector.empty
//
//    // Create a new client for each call to avoid serialization issues
//    try {
//      // Create a client without capturing the non-serializable ActorSystem
//      import scala.concurrent.ExecutionContext.Implicits.global
//      import akka.actor.ActorSystem
//
//      // Create a new ActorSystem per task - will be cleaned up by JVM
//      implicit val system: ActorSystem = ActorSystem("embedding-system-" + java.util.UUID.randomUUID().toString)
//      implicit val materializer = akka.stream.Materializer(system)
//
//      val client = OllamaClient(baseUrl)
//      val result = client.embed(texts, modelName)
//
//      // Shutdown the ActorSystem when done
//      system.terminate()
//
//      result
//    } catch {
//      case e: Exception =>
//        logger.error(s"Error generating embeddings: ${e.getMessage}", e)
//        throw e
//    }
//  }
//}

class SerializableEmbeddingGenerator(
                                      modelName: String,
                                      modelVersion: String,
                                      baseUrl: String = OllamaClient.detectOllamaHost()
                                    ) extends Serializable {

  @transient private lazy val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  def generateEmbeddings(texts: Vector[String]): Vector[Vector[Float]] = {
    if (texts.isEmpty) return Vector.empty
    // Reuse per-executor client & actor system
    import PerExecutor._
    implicit val sys  = system
    implicit val mat  = materializer
    implicit val ec   = sys.dispatcher

    // The call itself will still be sequentialized by the semaphore we add below
    client.embed(texts, modelName)
  }
}

