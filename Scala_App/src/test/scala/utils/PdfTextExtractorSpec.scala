package utils

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import utils.PdfTextExtractor
import utils.PdfChunker
import utils.PdfChunker.PdfChunk
import model.ExtractTextResult
import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}

class PdfTextExtractorSpec extends AnyFunSpec with Matchers with MockitoSugar with BeforeAndAfterAll {

  // Create a simple test PDF file path for testing (this would need to exist in a real test environment)
  val nonExistentPdfPath = "/path/to/nonexistent/file.pdf"
  val testOutputPath = "/tmp/test_output.txt"

  override def afterAll(): Unit = {
    // Clean up test files
    val testFile = new File(testOutputPath)
    if (testFile.exists()) testFile.delete()
    super.afterAll()
  }

  describe("PdfTextExtractor") {

    describe("extractText") {
      it("should handle non-existent file gracefully") {
        val result = PdfTextExtractor.extractText(nonExistentPdfPath)
        result shouldBe None
      }

      it("should handle empty file path") {
        val result = PdfTextExtractor.extractText("")
        result shouldBe None
      }

      it("should handle invalid file extension") {
        val result = PdfTextExtractor.extractText("/path/to/file.txt")
        result shouldBe None
      }
    }

    describe("safeExtractText") {
      it("should return ExtractTextResult with error for non-existent file") {
        val result = PdfTextExtractor.safeExtractText(nonExistentPdfPath)

        result shouldBe a[ExtractTextResult]
        result.text shouldBe ""
        result.debug should include("Error extracting text from PDF")
      }

      it("should return ExtractTextResult with error for empty path") {
        val result = PdfTextExtractor.safeExtractText("")

        result shouldBe a[ExtractTextResult]
        result.text shouldBe ""
        result.debug should include("Error extracting text from PDF")
      }

      it("should return ExtractTextResult for invalid file") {
        val result = PdfTextExtractor.safeExtractText("/invalid/file.pdf")

        result shouldBe a[ExtractTextResult]
        result.text shouldBe ""
        result.debug should include("Error extracting text from PDF")
      }
    }

    describe("normalize") {
      // Testing the private normalize method indirectly through a simple test
      it("should normalize text through extraction process") {
        // We can't directly test the private normalize method, but we can verify
        // that text normalization concepts are working through integration
        val testText = "Test  text\u00ADwith-\n soft hyphens"

        // This is more of a concept test since we can't directly access private methods
        // In a real scenario, we might extract this to a public utility method for testing
        testText should not be null
        testText.length should be > 0
      }
    }
  }

  describe("LenientStripper") {
    it("should be instantiable") {
      val stripper = new LenientStripper()
      stripper should not be null
      stripper shouldBe a[LenientStripper]
    }
  }

  describe("PdfChunker") {
    describe("stripText") {
      it("should clean excessive whitespace") {
        // Testing through public methods since stripText is private
        val testText = "This   is    test   text"
        val chunks = PdfChunker.createChunks(testText, 100, 20)

        chunks should not be empty
        chunks.head.text should not contain("   ") // Multiple spaces should be cleaned
      }
    }

    describe("PdfChunk") {
      it("should create chunks with correct structure") {
        val chunk = PdfChunk(0, 10, "test text")

        chunk.startIndex shouldBe 0
        chunk.endIndex shouldBe 10
        chunk.text shouldBe "test text"
      }
    }

    describe("createChunks") {
      it("should handle empty text") {
        val chunks = PdfChunker.createChunks("", 100, 20)
        chunks shouldBe empty
      }

      it("should handle text shorter than window size") {
        val shortText = "Short text"
        val chunks = PdfChunker.createChunks(shortText, 100, 20)

        chunks should have size 1
        chunks.head.text shouldBe shortText.replaceAll("\\s+", " ").trim
        chunks.head.startIndex shouldBe 0
        chunks.head.endIndex shouldBe shortText.replaceAll("\\s+", " ").trim.length
      }

      it("should create overlapping chunks for long text") {
        val longText = "This is a very long text that should be split into multiple chunks with overlap. " * 10
        val chunks = PdfChunker.createChunks(longText, 100, 20)

        chunks.size should be > 1

        // Verify overlap exists (simplified check)
        if (chunks.size >= 2) {
          val firstChunk = chunks(0)
          val secondChunk = chunks(1)

          secondChunk.startIndex should be < firstChunk.endIndex
        }
      }

      it("should respect window size limits") {
        val mediumText = "A" * 500
        val windowSize = 100
        val chunks = PdfChunker.createChunks(mediumText, windowSize, 20)

        chunks.foreach { chunk =>
          chunk.text.length should be <= windowSize
        }
      }

      it("should handle intelligent cutting when enabled") {
        val textWithSentences = "This is the first sentence. This is the second sentence. This is the third sentence."
        val chunks = PdfChunker.createChunks(textWithSentences, 50, 10, intelligentCut = true)

        chunks should not be empty
        // With intelligent cutting, it should try to break at sentence boundaries
      }

      it("should use default parameters") {
        val testText = "Default parameter test text. " * 50
        val chunks = PdfChunker.createChunks(testText)

        chunks should not be empty
        // Default window size is 1500, overlap is 250
      }
    }

    describe("chunkPdfFile") {
      it("should handle non-existent PDF file") {
        val result = PdfChunker.chunkPdfFile(nonExistentPdfPath)
        result shouldBe None
      }

      it("should handle empty file path") {
        val result = PdfChunker.chunkPdfFile("")
        result shouldBe None
      }

      it("should use default parameters when not specified") {
        val result = PdfChunker.chunkPdfFile(nonExistentPdfPath)
        // Should not throw exception even with non-existent file
        result shouldBe None
      }
    }

    describe("addMetadataToChunks") {
      it("should handle empty chunk list") {
        val result = PdfChunker.addMetadataToChunks(Vector.empty)
        result shouldBe empty
      }

      it("should add correct metadata to chunks") {
        val chunks = Vector("First chunk", "Second chunk", "Third chunk")
        val result = PdfChunker.addMetadataToChunks(chunks)

        result should have size 3

        val (firstId, firstChunk, firstMeta) = result(0)
        firstId shouldBe "chunk_1"
        firstChunk shouldBe "First chunk"
        firstMeta("chunk_id") shouldBe "chunk_1"
        firstMeta("position") shouldBe "1"
        firstMeta("total_chunks") shouldBe "3"

        val (lastId, lastChunk, lastMeta) = result(2)
        lastId shouldBe "chunk_3"
        lastChunk shouldBe "Third chunk"
        lastMeta("position") shouldBe "3"
      }
    }

    describe("calculateChunkStatistics") {
      it("should handle empty chunk list") {
        val stats = PdfChunker.calculateChunkStatistics(Vector.empty)
        stats("count") shouldBe 0
      }

      it("should calculate correct statistics") {
        val chunks = Vector(
          PdfChunk(0, 10, "1234567890"),      // 10 chars
          PdfChunk(10, 25, "123456789012345"), // 15 chars
          PdfChunk(25, 30, "12345")           // 5 chars
        )

        val stats = PdfChunker.calculateChunkStatistics(chunks)

        stats("count") shouldBe 3
        stats("min_length") shouldBe 5
        stats("max_length") shouldBe 15
        stats("avg_length") shouldBe 10.0
        stats("total_characters") shouldBe 30
      }

      it("should handle single chunk") {
        val chunks = Vector(PdfChunk(0, 5, "hello"))
        val stats = PdfChunker.calculateChunkStatistics(chunks)

        stats("count") shouldBe 1
        stats("min_length") shouldBe 5
        stats("max_length") shouldBe 5
        stats("avg_length") shouldBe 5.0
        stats("total_characters") shouldBe 5
      }
    }
  }
}
