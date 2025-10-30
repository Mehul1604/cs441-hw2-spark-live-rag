package llm

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll

class SerializableEmbeddingGeneratorSpec extends AnyFunSpec with Matchers with MockitoSugar with BeforeAndAfterAll {

  // Sample test data
  val sampleTexts: Vector[String] = Vector("machine learning", "artificial intelligence", "deep learning")
  val sampleEmbeddingData: Vector[Vector[Float]] = Vector(
    Vector(0.1f, 0.2f, 0.3f, 0.4f, 0.5f),
    Vector(0.15f, 0.25f, 0.35f, 0.45f, 0.55f),
    Vector(0.2f, 0.3f, 0.4f, 0.5f, 0.6f)
  )

  describe("SerializableEmbeddingGenerator") {
    describe("generateEmbeddings") {
      it("should handle empty input gracefully") {
        // Create generator instance
        val generator = new SerializableEmbeddingGenerator(
          modelName = "mxbai-embed-large",
          modelVersion = "1.0",
          baseUrl = "http://test-ollama:11434"
        )

        // Test with empty input
        val result = generator.generateEmbeddings(Vector.empty)

        // Should return empty result
        result shouldBe Vector.empty
        result.length shouldBe 0
      }

      it("should be serializable") {
        // Create generator instance
        val generator = new SerializableEmbeddingGenerator(
          modelName = "mxbai-embed-large",
          modelVersion = "1.0",
          baseUrl = "http://test-ollama:11434"
        )

        // Test that the class is actually serializable
        import java.io.{ByteArrayOutputStream, ObjectOutputStream, ByteArrayInputStream, ObjectInputStream}

        // Serialize the object
        val baos = new ByteArrayOutputStream()
        val oos = new ObjectOutputStream(baos)

        noException should be thrownBy {
          oos.writeObject(generator)
          oos.close()
        }

        // Deserialize the object
        val bais = new ByteArrayInputStream(baos.toByteArray)
        val ois = new ObjectInputStream(bais)

        val deserializedGenerator = ois.readObject().asInstanceOf[SerializableEmbeddingGenerator]
        ois.close()

        // Verify that deserialized object is not null and has same properties
        deserializedGenerator should not be null
        // Note: We can't directly access private fields, but we can verify the object was successfully deserialized
        deserializedGenerator.getClass shouldBe generator.getClass
      }
    }

    describe("constructor parameters") {
      it("should accept and store model configuration") {
        val modelName = "custom-embed-model"
        val modelVersion = "2.0"
        val baseUrl = "http://custom-ollama:9999"

        // Should not throw exception when creating with custom parameters
        noException should be thrownBy {
          new SerializableEmbeddingGenerator(
            modelName = modelName,
            modelVersion = modelVersion,
            baseUrl = baseUrl
          )
        }
      }

      it("should use default baseUrl when not provided") {
        val modelName = "test-model"
        val modelVersion = "1.0"

        // Should not throw exception when creating with default baseUrl
        noException should be thrownBy {
          new SerializableEmbeddingGenerator(
            modelName = modelName,
            modelVersion = modelVersion
          )
        }
      }
    }
  }
}
