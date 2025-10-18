package utils

import org.slf4j.LoggerFactory
import opennlp.tools.langdetect.{LanguageDetectorME, LanguageDetectorModel}
import java.io.Serializable
import scala.util.Try

/**
 * Language detection utilities for text processing
 */
object LanguageDetector {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Wrapper class for language detection results
   * @param lang The detected language code
   */
  case class LanguageResult(lang: String) extends Serializable

  /**
   * Serializable language detector service for use in distributed computing environments like Spark
   */
  class SerializableLanguageDetector extends Serializable {
    @transient private lazy val langDetectModel = {
      Try {
        new LanguageDetectorModel(
          getClass.getResourceAsStream("/models/langdetect-183.bin")
        )
      }.getOrElse(null)
    }

    /**
     * Detect the language of the provided text
     * @param text The input text to analyze
     * @return LanguageResult containing the detected language code or "unknown"
     */
    def detectLanguage(text: String): LanguageResult = {
      if (text == null || text.isEmpty) {
        return LanguageResult("unknown")
      }

      try {
        if (langDetectModel != null) {
          val detector = new LanguageDetectorME(langDetectModel)
          val bestLang = detector.predictLanguage(text)
          LanguageResult(bestLang.getLang)
        } else {
          // Fallback if model couldn't be loaded
          logger.warn("Language detection model could not be loaded - returning unknown")
          LanguageResult("unknown")
        }
      } catch {
        case e: Exception =>
          logger.error(s"Language detection failed: ${e.getMessage}")
          LanguageResult("unknown")
      }
    }

    /**
     * Detect language for a potentially large text by sampling the first N characters
     * @param text The input text
     * @param sampleSize Maximum number of characters to process
     * @return LanguageResult containing the detected language code
     */
    def detectLanguageWithSampling(text: String, sampleSize: Int = 1000): LanguageResult = {
      if (text == null || text.isEmpty) {
        return LanguageResult("unknown")
      }

      // Take a sample of the text to avoid excessive processing
      val sampleText = if (text.length > sampleSize) text.substring(0, sampleSize) else text
      detectLanguage(sampleText)
    }
  }
}
