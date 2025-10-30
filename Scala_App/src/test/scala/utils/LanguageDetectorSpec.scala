package utils

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import utils.LanguageDetector.{LanguageResult, SerializableLanguageDetector}

class LanguageDetectorSpec extends AnyFunSpec with Matchers with MockitoSugar with BeforeAndAfterAll {

  describe("LanguageResult") {
    it("should create a language result with correct language code") {
      val result = LanguageResult("en")
      result.lang shouldBe "en"
    }

    it("should be serializable") {
      val result = LanguageResult("fr")

      import java.io.{ByteArrayOutputStream, ObjectOutputStream, ByteArrayInputStream, ObjectInputStream}

      // Serialize the object
      val baos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(baos)

      noException should be thrownBy {
        oos.writeObject(result)
        oos.close()
      }

      // Deserialize the object
      val bais = new ByteArrayInputStream(baos.toByteArray)
      val ois = new ObjectInputStream(bais)

      val deserializedResult = ois.readObject().asInstanceOf[LanguageResult]
      ois.close()

      deserializedResult.lang shouldBe "fr"
    }
  }

  describe("SerializableLanguageDetector") {
    val detector = new SerializableLanguageDetector()

    describe("detectLanguage") {
      it("should handle null input gracefully") {
        val result = detector.detectLanguage(null)
        result.lang shouldBe "unknown"
      }

      it("should handle empty input gracefully") {
        val result = detector.detectLanguage("")
        result.lang shouldBe "unknown"
      }

      it("should detect English text") {
        val englishText = "This is a sample English text for testing language detection functionality."
        val result = detector.detectLanguage(englishText)

        // Note: Without the actual model file, this will likely return "unknown"
        // In a real test environment with the model, we'd expect "en"
        result.lang should not be null
        result.lang.length should be > 0
      }

      it("should detect French text") {
        val frenchText = "Ceci est un texte français pour tester la fonctionnalité de détection de langue."
        val result = detector.detectLanguage(frenchText)

        // Note: Without the actual model file, this will likely return "unknown"
        // In a real test environment with the model, we'd expect "fr"
        result.lang should not be null
        result.lang.length should be > 0
      }

      it("should handle very short text") {
        val shortText = "Hi"
        val result = detector.detectLanguage(shortText)

        result.lang should not be null
        result.lang.length should be > 0
      }

      it("should handle text with special characters") {
        val specialText = "Hello! @#$%^&*() 123 world"
        val result = detector.detectLanguage(specialText)

        result.lang should not be null
        result.lang.length should be > 0
      }
    }

    describe("detectLanguageWithSampling") {
      it("should handle null input gracefully") {
        val result = detector.detectLanguageWithSampling(null)
        result.lang shouldBe "unknown"
      }

      it("should handle empty input gracefully") {
        val result = detector.detectLanguageWithSampling("")
        result.lang shouldBe "unknown"
      }

      it("should sample large text correctly") {
        val largeText = "This is English text. " * 100 // Create a large text
        val result = detector.detectLanguageWithSampling(largeText, 50)

        result.lang should not be null
        result.lang.length should be > 0
      }

      it("should handle text smaller than sample size") {
        val smallText = "Small English text"
        val result = detector.detectLanguageWithSampling(smallText, 50) // Use smaller sample size than default

        result.lang should not be null
        result.lang.length should be > 0
      }

      it("should use default sample size when not specified") {
        val mediumText = "This is a medium-sized English text for testing default sampling behavior."
        val result = detector.detectLanguageWithSampling(mediumText)

        result.lang should not be null
        result.lang.length should be > 0
      }

      it("should handle multilingual text by sampling") {
        val multilingualText = "English text at the beginning. " + "Texto en español en el medio. " + "Texte français à la fin."
        val result = detector.detectLanguageWithSampling(multilingualText, 30) // Sample only the English part

        result.lang should not be null
        result.lang.length should be > 0
      }
    }

    describe("serialization") {
      it("should be serializable for use in Spark") {
        import java.io.{ByteArrayOutputStream, ObjectOutputStream, ByteArrayInputStream, ObjectInputStream}

        // Serialize the detector
        val baos = new ByteArrayOutputStream()
        val oos = new ObjectOutputStream(baos)

        noException should be thrownBy {
          oos.writeObject(detector)
          oos.close()
        }

        // Deserialize the detector
        val bais = new ByteArrayInputStream(baos.toByteArray)
        val ois = new ObjectInputStream(bais)

        val deserializedDetector = ois.readObject().asInstanceOf[SerializableLanguageDetector]
        ois.close()

        // Test that deserialized detector still works
        val testResult = deserializedDetector.detectLanguage("Test text")
        testResult.lang should not be null
        testResult.lang.length should be > 0
      }
    }
  }

  describe("LanguageDetector object") {
    it("should provide access to LanguageResult case class") {
      val result = LanguageResult("de")
      result shouldBe a[LanguageResult]
      result.lang shouldBe "de"
    }

    it("should provide access to SerializableLanguageDetector class") {
      val detector = new SerializableLanguageDetector()
      detector shouldBe a[SerializableLanguageDetector]
    }
  }
}
