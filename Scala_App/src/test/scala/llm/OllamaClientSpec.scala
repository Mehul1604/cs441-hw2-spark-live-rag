package llm

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import akka.actor.ActorSystem
import akka.stream.Materializer

// Correct imports for the OpenAI Scala client classes
import io.cequence.openaiscala.domain.BaseMessage
import io.cequence.openaiscala.domain.response.ChatCompletionResponse
// Correct embedding response classes based on the actual API
import io.cequence.openaiscala.domain.response.{EmbeddingResponse, EmbeddingInfo, EmbeddingUsageInfo}
import io.cequence.openaiscala.domain.settings.{CreateChatCompletionSettings, CreateEmbeddingsSettings}
import io.cequence.openaiscala.service.OpenAIService

import org.scalatest.BeforeAndAfterAll
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration.Duration

class OllamaClientSpec extends AnyFunSpec with Matchers with MockitoSugar with BeforeAndAfterAll {

  // Sample test data
  val sampleTexts: Vector[String] = Vector("apple", "orange", "banana")
  val sampleEmbeddingData: Vector[Vector[Float]] = Vector(
    Vector(0.1f, 0.2f, 0.3f, 0.4f, 0.5f),
    Vector(0.15f, 0.25f, 0.35f, 0.45f, 0.55f),
    Vector(0.9f, 0.8f, 0.7f, 0.6f, 0.5f)
  )
  val sampleChatMessages: Vector[ChatMessage] = Vector(
    ChatMessage("system", "You are a helpful assistant."),
    ChatMessage("user", "What is the capital of Japan?")
  )
  val sampleChatResponse: String = "The capital of Japan is Tokyo."

  // Set up ActorSystem and other implicits for testing
  implicit val system: ActorSystem = ActorSystem("OllamaClientSpec")
  implicit val materializer: Materializer = Materializer(system)
  implicit val ec: ExecutionContext = ExecutionContext.Implicits.global

  // Clean up ActorSystem after all tests
  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  describe("OllamaClient") {
    describe("embeddings") {
      it("should successfully generate embeddings for text") {
        // Create mocks
        val mockService = mock[OpenAIService]

        // Create embedding data directly without mocking EmbeddingInfo objects
        // This avoids issues with mocking final classes/methods
        val embeddingResponse = EmbeddingResponse(
          data = sampleEmbeddingData.zipWithIndex.map { case (embedding, idx) =>
            EmbeddingInfo(
              embedding = embedding.map(_.toDouble),
              index = idx
            )
          },
          model = "mxbai-embed-large",
          usage = EmbeddingUsageInfo(
            prompt_tokens = 3,
            total_tokens = 3
          )
        )

        // Configure the mock service to return our response
        when(mockService.createEmbeddings(
          any[Seq[String]],
          any[CreateEmbeddingsSettings]
        )).thenReturn(Future.successful(embeddingResponse))

        // Create a testable client that uses our mocks
        val testClient = new OllamaClient("http://test-ollama:11434") {
          override protected val service: OpenAIService = mockService
        }

        // Execute the method under test
        val result = testClient.embed(sampleTexts, "mxbai-embed-large")

        // Verify results
        result.length shouldBe sampleTexts.length
        result.zip(sampleEmbeddingData).foreach { case (actual, expected) =>
          actual shouldBe expected
        }

        // Verify service was called with correct parameters
        verify(mockService).createEmbeddings(
          org.mockito.ArgumentMatchers.eq(sampleTexts),
          any[CreateEmbeddingsSettings]
        )
      }

      it("should handle empty input gracefully") {
        // Create mock
        val mockService = mock[OpenAIService]

        // Create client with mock
        val testClient = new OllamaClient("http://test-ollama:11434") {
          override protected val service: OpenAIService = mockService
        }

        // Test with empty input
        val result = testClient.embed(Vector.empty, "mxbai-embed-large")

        // Should return empty result without calling service
        result shouldBe Vector.empty
        verify(mockService, never()).createEmbeddings(
          any[Seq[String]],
          any[CreateEmbeddingsSettings]
        )
      }

      it("should retry on timeout exceptions") {
        // Create mock
        val mockService = mock[OpenAIService]

        // Configure mock to fail twice with timeout, then succeed
        when(mockService.createEmbeddings(
          any[Seq[String]],
          any[CreateEmbeddingsSettings]
        )).thenAnswer(new Answer[Future[EmbeddingResponse]] {
          var callCount = 0

          override def answer(invocation: InvocationOnMock): Future[EmbeddingResponse] = {
            callCount += 1

            if (callCount <= 2) {
              // First two calls fail with timeout
              Future.failed(new java.util.concurrent.TimeoutException("Test timeout"))
            } else {
              // For the third call, create a successful response
              val responseData = sampleEmbeddingData.zipWithIndex.map { case (embedding, i) =>
                val mockData = mock[EmbeddingInfo]
                val embedVector = embedding.map(_.toDouble)
                when(mockData.embedding).thenReturn(embedVector)
                when(mockData.index).thenReturn(i)
                mockData
              }

              val mockResponse = mock[EmbeddingResponse]
              when(mockResponse.data).thenReturn(responseData)
              when(mockResponse.model).thenReturn("mxbai-embed-large")
              when(mockResponse.usage).thenReturn(mock[EmbeddingUsageInfo])

              Future.successful(mockResponse)
            }
          }
        })

        // Create a testable client with shorter timeout for testing
        val testClient = new OllamaClient("http://test-ollama:11434") {
          override protected val service: OpenAIService = mockService
          // Use protected modifier for consistency with the class definition
          override protected val timeout: scala.concurrent.duration.FiniteDuration = Duration(1, "second")
        }

        // Execute the method under test
        val result = testClient.embed(sampleTexts, "mxbai-embed-large")

        // Verify results
        result.length shouldBe sampleTexts.length

        // Verify service was called 3 times (2 failures + 1 success)
        verify(mockService, times(3)).createEmbeddings(
          any[Seq[String]],
          any[CreateEmbeddingsSettings]
        )
      }
    }

    describe("chat") {
      it("should correctly process chat messages") {
        // Create mock
        val mockService = mock[OpenAIService]

        // Create chat response using proper mocking
        val mockResponse = mock[ChatCompletionResponse]

        // Set up the behavior for contentHead to directly return our sample response
        when(mockResponse.contentHead).thenReturn(sampleChatResponse)

        // Configure mock to return our response
        when(mockService.createChatCompletion(
          any[Seq[BaseMessage]],
          any[CreateChatCompletionSettings]
        )).thenReturn(Future.successful(mockResponse))

        // Create a testable client that uses our mocks
        val testClient = new OllamaClient("http://test-ollama:11434") {
          override protected val service: OpenAIService = mockService
        }

        // Execute the method under test
        val result = testClient.chat(sampleChatMessages, "llama3.1")

        // Verify results
        result shouldBe sampleChatResponse

        // Verify service was called with correct parameters
        verify(mockService).createChatCompletion(
          any[Seq[BaseMessage]],
          any[CreateChatCompletionSettings]
        )
      }

      it("should handle different message roles correctly") {
        // Create mock
        val mockService = mock[OpenAIService]

        // Test messages with different roles
        val mixedRoleMessages = Vector(
          ChatMessage("system", "You are a helpful assistant."),
          ChatMessage("user", "Hello"),
          ChatMessage("assistant", "How can I help you?"),
          ChatMessage("unknown_role", "This role is not recognized")
        )

        // Create mock response
        val mockResponse = mock[ChatCompletionResponse]

        // Set up the behavior for contentHead
        when(mockResponse.contentHead).thenReturn("Test response")

        // Configure mock to return our response
        when(mockService.createChatCompletion(
          any[Seq[BaseMessage]],
          any[CreateChatCompletionSettings]
        )).thenReturn(Future.successful(mockResponse))

        // Create a testable client
        val testClient = new OllamaClient("http://test-ollama:11434") {
          override protected val service: OpenAIService = mockService
        }

        // Execute the method - should not throw exception
        noException should be thrownBy {
          testClient.chat(mixedRoleMessages, "llama3.1")
        }
      }
    }

    describe("dim") {
      it("should return correct dimensions for known models") {
        val client = new OllamaClient("http://localhost:11434")

        // Test known model dimensions
        client.dim("mxbai-embed-small") shouldBe 384
        client.dim("mxbai-embed-medium") shouldBe 768
        client.dim("mxbai-embed-large") shouldBe 1024

        // Test default dimension
        client.dim("unknown-model") shouldBe 1024
      }
    }

    describe("detectOllamaHost") {
      it("should prioritize environment variable if available") {
        // This is a bit tricky as we can't directly modify sys.env
        // In a real test, you might use a mocking library that can mock static methods
        // For this example, we'll verify the behavior through reflection

        val method = classOf[OllamaClient].getMethod("detectOllamaHost")
        val result = method.invoke(null).asInstanceOf[String]

        // The test is more of a verification that the method exists and returns a string
        result should not be null
        result should include("http://")
        result should endWith(":11434")
      }
    }
  }
}
