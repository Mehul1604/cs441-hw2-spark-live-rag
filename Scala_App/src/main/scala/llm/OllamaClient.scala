package llm

import akka.actor.ActorSystem
import akka.stream.Materializer
import io.cequence.openaiscala.domain._
import io.cequence.openaiscala.domain.settings.{CreateChatCompletionSettings, CreateEmbeddingsSettings}
import io.cequence.openaiscala.service.{OpenAIChatCompletionService, OpenAIChatCompletionServiceFactory, OpenAIService, OpenAIServiceFactory}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import java.net.{HttpURLConnection, URL}
import java.lang.Thread // Add explicit import for Thread
import org.slf4j.LoggerFactory

final case class ChatMessage(role: String, content: String)

class OllamaClient(baseUrl: String = OllamaClient.detectOllamaHost())
                  (
                    implicit ec: ExecutionContext,
                    materializer: Materializer
                  ) extends Embeddings {

  private val logger = LoggerFactory.getLogger(this.getClass)

  logger.info(s"Initializing OllamaClient with base URL: $baseUrl")

  // Configure the openai-scala-client to use the Ollama API
  protected val service: OpenAIService = OpenAIServiceFactory.customInstance(
    coreUrl = s"$baseUrl/v1/"
  )

  private val chatCompletionService: OpenAIChatCompletionService = OpenAIChatCompletionServiceFactory(
    coreUrl = s"$baseUrl/v1/"
  )

  protected val timeout: FiniteDuration = 180.seconds // Increased timeout for EMR
  private val batchSize: Int = 10 // Optimal batch size for EMR resources

  // add optional batching parameter which says if batching is applied or not
  def embed(texts: Vector[String], model: String, batching: Boolean = true): Vector[Vector[Float]] = {
    logger.debug(s"Generating embeddings for ${texts.length} texts using model: $model")

    if (texts.isEmpty) {
      logger.debug("Empty input texts, returning empty embeddings")
      return Vector.empty
    }

    if (!batching) {
      val allEmbeddings = processBatchWithRetry(texts, model, 1, 1)
      logger.info(s"Successfully generated ${allEmbeddings.length} total embeddings without batching")
      return allEmbeddings
    }
    // Process in batches to avoid overwhelming Ollama and reduce timeout risk
    val batches = texts.grouped(batchSize).toVector
    logger.info(s"Processing ${texts.length} texts in ${batches.length} batches of size $batchSize")

    val allEmbeddings = batches.zipWithIndex.flatMap { case (batch, batchIndex) =>
      processBatchWithRetry(batch, model, batchIndex + 1, batches.length)
    }

    logger.debug(s"Successfully generated ${allEmbeddings.length} total embeddings")
    allEmbeddings
  }

  private def processBatchWithRetry(
                                     batch: Vector[String],
                                     model: String,
                                     batchNumber: Int,
                                     totalBatches: Int,
                                     maxRetries: Int = 3
                                   ): Vector[Vector[Float]] = {
    var attempt = 0
    var lastException: Exception = null

    while (attempt < maxRetries) {
      try {
        attempt += 1
        val startTime = System.currentTimeMillis()

        logger.info(s"Processing batch $batchNumber/$totalBatches (${batch.length} texts, attempt $attempt/$maxRetries)")

        val embedFut = service.createEmbeddings(
          input = batch,
          settings = CreateEmbeddingsSettings(model),

        ).map { response =>
          response.data.map { embeddingData =>
            embeddingData.embedding.map(_.toFloat).toVector
          }.toVector
        }

        val result = Await.result(embedFut, timeout)
        val duration = System.currentTimeMillis() - startTime

        logger.info(s"✅ Batch $batchNumber completed successfully in ${duration}ms (${result.length} embeddings)")

        // Small delay between batches to avoid overwhelming Ollama
        if (batchNumber < totalBatches) {
          Thread.sleep(500) // 500ms pause between batches
        }

        return result

      } catch {
        case e: java.util.concurrent.TimeoutException =>
          lastException = e
          logger.warn(s"⏰ Batch $batchNumber timeout on attempt $attempt/$maxRetries (${timeout.toSeconds}s limit)")
          if (attempt < maxRetries) {
            val backoffDelay = 5000 * attempt // Exponential backoff: 5s, 10s, 15s
            logger.info(s"💤 Waiting ${backoffDelay}ms before retry...")
            Thread.sleep(backoffDelay)
          }
        case e: Exception =>
          logger.error(s"❌ Batch $batchNumber failed with non-timeout error: ${e.getMessage}", e)
          throw e
      }
    }

    logger.error(s"💥 Batch $batchNumber failed after $maxRetries attempts")
    throw new RuntimeException(s"Batch processing failed after $maxRetries retries", lastException)
  }

  override def dim(model: String): Int =
    model match {
      case "mxbai-embed-small" => 384
      case "mxbai-embed-medium" => 768
      case "mxbai-embed-large" => 1024
      case _ => 1024
    }

  def chat(messages: Vector[ChatMessage], model: String): String = {
    logger.debug(s"Starting chat with ${messages.length} messages using model: $model")

    try {
      val chatFut = service.createChatCompletion(
        messages = messages.map { m =>
          m.role match {
            case "user" => UserMessage(content = m.content)
            case "system" => SystemMessage(content = m.content)
            case "assistant" => AssistantMessage(content = m.content)
            case _ => UserMessage(content = m.content)
          }
        },
        settings = CreateChatCompletionSettings(
          model = model,
          temperature = Some(0.1)
        )
      ).map(_.contentHead)

      val result = Await.result(chatFut, timeout)
      logger.debug(s"Chat completed successfully")
      result
    } catch {
      case e: Exception =>
        logger.error(s"Chat failed: ${e.getMessage}", e)
        throw e
    }
  }
}

object OllamaClient {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Detects the appropriate Ollama host URL based on environment and connectivity
   */
  def detectOllamaHost(): String = {
    // Priority order for host detection:
    // 1. Environment variable OLLAMA_HOST
    // 2. Try local instance IP (for EMR)
    // 3. Fallback to localhost

    val envHost = sys.env.get("OLLAMA_HOST")
    if (envHost.isDefined) {
      logger.info(s"Using Ollama host from environment: ${envHost.get}")
      return envHost.get
    }

    // Try to get EC2 instance private IP (for EMR)
    val instanceIp = Try {
      val url = new URL("http://169.254.169.254/latest/meta-data/local-ipv4")
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setConnectTimeout(2000)
      connection.setReadTimeout(2000)
      val inputStream = connection.getInputStream
      val ip = scala.io.Source.fromInputStream(inputStream).mkString.trim
      inputStream.close()
      ip
    }.toOption

    val candidateHosts = instanceIp match {
      case Some(ip) =>
        logger.info(s"Detected EC2 instance IP: $ip")
        Seq(s"http://$ip:11434", "http://localhost:11434")
      case None =>
        logger.info("Not running on EC2, using localhost")
        Seq("http://localhost:11434")
    }

    // Test connectivity to each candidate host
    for (host <- candidateHosts) {
      if (testOllamaConnectivity(host)) {
        logger.info(s"Successfully connected to Ollama at: $host")
        return host
      }
    }

    // Fallback to localhost if nothing works
    val fallback = "http://localhost:11434"
    logger.warn(s"No Ollama connectivity detected, using fallback: $fallback")
    fallback
  }

  /**
   * Tests connectivity to an Ollama instance
   */
  private def testOllamaConnectivity(host: String): Boolean = {
    try {
      val url = new URL(s"$host/api/tags")
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setConnectTimeout(3000)
      connection.setReadTimeout(3000)
      connection.setRequestMethod("GET")

      val responseCode = connection.getResponseCode
      connection.disconnect()

      responseCode == 200
    } catch {
      case _: Exception => false
    }
  }

  def apply()(implicit ec: ExecutionContext, materializer: Materializer): OllamaClient = {
    new OllamaClient(detectOllamaHost())
  }

  def apply(baseUrl: String)(implicit ec: ExecutionContext, materializer: Materializer): OllamaClient = {
    new OllamaClient(baseUrl)
  }
}

object OllamaTest extends App {
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: Materializer = Materializer(system)
  implicit val ec: ExecutionContext = ExecutionContext.Implicits.global

  val ollamaClient = OllamaClient()

  println("Starting chat and embeddings tests...")

  // --- Test Chat ---
  val chatMessages = Vector(
    ChatMessage("system", "You are a helpful assistant."),
    ChatMessage("user", "What is the capital of Japan?")
  )

  try {
    val chatResponse = ollamaClient.chat(chatMessages, "llama3.1")
    println(s"\nChat Response:\n$chatResponse")
  } catch {
    case e: Exception =>
      println(s"Chat test failed: ${e.getMessage}")
  }

  // --- Test Embeddings ---
  val textsToEmbed = Vector("apple", "orange", "pineapple")

  try {
    val embeddings = ollamaClient.embed(textsToEmbed, "mxbai-embed-large")
    println(s"\nEmbeddings Response:")
    embeddings.zip(textsToEmbed).foreach { case (embedding, text) =>
      println(s"Text: '$text' -> Embedding snippet: [${embedding.take(5).mkString(", ")}...] | Embedding length - [${embedding.length}]")
    }
  } catch {
    case e: Exception =>
      println(s"Embeddings test failed: ${e.getMessage}")
  }

  println("\nTests finished.")
  system.terminate()

}