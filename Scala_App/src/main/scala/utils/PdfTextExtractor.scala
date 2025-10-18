package utils

import org.apache.pdfbox.Loader
import org.apache.pdfbox.text.{PDFTextStripper, TextPosition}
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.AutoDetectParser
import org.apache.tika.sax.BodyContentHandler

import java.io.{File, FileInputStream, PrintWriter}
import scala.util.Using

class LenientStripper extends PDFTextStripper {
  override protected def processTextPosition(tp: TextPosition): Unit = {
    try {
      val c = tp.getUnicode
      if (c != null && c.nonEmpty) super.processTextPosition(tp)
    } catch {
      case _: IllegalArgumentException =>
      // skip problematic glyph (e.g., space in CMSY font)
    }
  }
}

object PdfTextExtractor {
  def extractTextAndMetadata(pdfPath: String): Option[String] = {
    val file = new File(pdfPath)
    val inputstream = new FileInputStream(file)
    val handler = new BodyContentHandler(-1) // -1 for unlimited content size
    val metadata = new Metadata()
    val parser = new AutoDetectParser()

    try {
      parser.parse(inputstream, handler, metadata)
      Some(handler.toString())
    } catch {
      case e: Exception =>
        println(s"Error extracting text from PDF: ${e.getMessage}")
        None
    }
    finally {
      inputstream.close()
    }
  }

  def extractText(filePath: String): Option[String] = {

    try {
      Using.resource(Loader.loadPDF(new File(filePath))) { doc =>
        val stripper = new LenientStripper()

        stripper.setSuppressDuplicateOverlappingText(true) // fixes double-drawn glyphs
        stripper.setShouldSeparateByBeads(true) // respect article beads (columns)
//        stripper.setSortByPosition(true)


        val fullText = (1 to doc.getNumberOfPages).toVector.map { pNo =>
          try {
            stripper.setStartPage(pNo)
            stripper.setEndPage(pNo)
            stripper.getText(doc)
          } catch {
            case e: IllegalArgumentException =>
              println(s"Skipping page $pNo due to font mapping error: ${e.getMessage}")
              ""
          }
        }.mkString("======\n\n======")

        Some(normalize(fullText))
      }
    } catch {
      case e: Exception =>
        println(s"Error extracting text from PDF: ${e.getMessage}")
        e.printStackTrace()
        None
    }
  }

  private def normalize(s: String): String =
    s
      // Remove soft hyphens but keep newlines (removed the de-hyphenation step)
//      .replace("\u00AD", "")
      // Remove soft hyphens and de-hyphenate line breaks
      .replace("\u00AD", "")
      .replaceAll("(?m)-\\s*\\n\\s*", "")
      // Normalize special spaces
      .replaceAll("[\\u00A0\\u2007\\u202F]", " ")
      // Normalize ligatures that some fonts emit
      .replace("\uFB00", "ff").replace("\uFB01", "fi").replace("\uFB02", "fl")
      .replace("\uFB03", "ffi").replace("\uFB04", "ffl")
      // Remove all non-Unicode (non-printable) characters, but preserve newlines
      .replaceAll("[^\u0020-\u007E\u00A0-\uFFFF\n]", " ")
      // Collapse excessive whitespace but preserve newlines
      .replaceAll("[ \t\r]+", " ")
      .trim

  def main(args: Array[String]): Unit = {
    val pdfFilePath = "/Users/mehulmathur/UIC/Cloud/Project/cs441-hw2-spark-live-rag/data/text_corpus/1083142.1083143.pdf" // Replace with your PDF file path
    val outputFilePath = "/Users/mehulmathur/UIC/Cloud/Project/cs441-hw2-spark-live-rag/data/extracted_text.txt" // Output text file path

    val writer = new PrintWriter(new File(outputFilePath))
    extractText(pdfFilePath) match {
      case Some(text) =>
        writer.write(text)
      case None => println("Could not extract text.")
    }
  }
}

object PdfChunker {
  /**
   * Clean text by removing excessive whitespace and trimming
   * @param text The input text to normalize
   * @return Cleaned text with single spaces
   */
  private def stripText(text: String): String = text.replaceAll("\\s+", " ").trim

  // a case class to hold chunk information - start position, end position, and the chunk text
  case class PdfChunk(startIndex: Int, endIndex: Int, text: String)

  /**
   * Create chunks from text using a sliding window approach
   * @param text The input text to chunk
   * @param windowSize Maximum size of each chunk in characters (default: 1500)
   * @param overlapSize Number of characters to overlap between chunks (default: 250)
   * @param intelligentCut Whether to try to cut at natural boundaries like end of sentences (default: true)
   * @param minCutThreshold Minimum threshold for acceptable cuts as a fraction of window size (default: 0.6)
   * @return Vector of text chunks
   */
  def createChunks(
                    text: String,
                    windowSize: Int = 1500,
                    overlapSize: Int = 250,
                    intelligentCut: Boolean = false,
                    minCutThreshold: Double = 0.6
                  ): Vector[PdfChunk] = {
    // Normalize the text first
    val cleanText = stripText(text)
    val chunks = Vector.newBuilder[PdfChunk]

    // Calculate stride (how much to move forward after each chunk)
    val stride = windowSize - overlapSize

    // Position tracker
    var position = 0

    // Process until we've covered the entire text
    while (position < cleanText.length) {
      // Calculate the end of the current window (capped by text length)
      val endPosition = Math.min(position + windowSize, cleanText.length)
      val currentSlice = cleanText.substring(position, endPosition)

      // If intelligentCut is enabled, try to find a natural breaking point
      val chunk = if (intelligentCut && currentSlice.length > minCutThreshold * windowSize) {
        // Find a proper sentence ending
        // This looks for periods, question marks, or exclamation points followed by a space and uppercase letter
        // or followed by a space and end of text, or followed by newline
        val sentenceEndRegex = """[.!?](?=\s+[A-Z]|\s*$|\n)""".r
        val matches = sentenceEndRegex.findAllMatchIn(currentSlice).toList

        if (matches.nonEmpty) {
          // Find the last sentence end that is past our minimum threshold
          val validMatches = matches.filter(m => m.end >= (windowSize * minCutThreshold).toInt)

          if (validMatches.nonEmpty) {
            // Use the last valid sentence ending
            val lastValidMatch = validMatches.last
            currentSlice.substring(0, lastValidMatch.end)
          } else {
            // Fall back to paragraph break if available
            val lastParagraphBreak = currentSlice.lastIndexOf("\n\n")
            if (lastParagraphBreak >= (windowSize * minCutThreshold).toInt) {
              currentSlice.substring(0, lastParagraphBreak + 2)  // Include the newlines
            } else {
              // No good sentence or paragraph breaks, use the whole slice
              currentSlice
            }
          }
        } else {
          // No sentence endings found, check for paragraph breaks
          val lastParagraphBreak = currentSlice.lastIndexOf("\n\n")
          if (lastParagraphBreak >= (windowSize * minCutThreshold).toInt) {
            currentSlice.substring(0, lastParagraphBreak + 2)  // Include the newlines
          } else {
            // No good sentence or paragraph breaks, use the whole slice
            currentSlice
          }
        }
      } else {
        // No intelligent cutting, just use the whole slice
        currentSlice
      }

      // Add the chunk to our collection with start and end indices
      chunks += PdfChunk(position, position + chunk.length, chunk)

      // Calculate the next position
      // If we used intelligent cutting, adjust the stride based on the actual chunk size
      // Otherwise, use the predetermined stride
      val nextPosition = if (intelligentCut && chunk.length < currentSlice.length) {
        position + chunk.length - overlapSize
      } else {
        position + stride
      }

      // Ensure we make progress even with very small chunks or large overlap
      position = Math.max(position + 1, nextPosition)
    }

    chunks.result()
  }

  /**
   * Create chunks from a PDF file
   * @param pdfPath Path to the PDF file
   * @param windowSize Maximum size of each chunk in characters
   * @param overlapSize Number of characters to overlap between chunks
   * @return Option containing a Vector of text chunks, or None if extraction failed
   */
  def chunkPdfFile(
                    pdfPath: String,
                    windowSize: Int = 1500,
                    overlapSize: Int = 250
                  ): Option[Vector[PdfChunk]] = {
    PdfTextExtractor.extractText(pdfPath).map { text =>
      createChunks(text, windowSize, overlapSize)
    }
  }

  /**
   * Generate metadata for each chunk, including position information and chunk ID
   * @param chunks Vector of text chunks
   * @return Vector of tuples (chunkId, chunk, metadata)
   */
  def addMetadataToChunks(chunks: Vector[String]): Vector[(String, String, Map[String, String])] = {
    chunks.zipWithIndex.map { case (chunk, index) =>
      val chunkId = s"chunk_${index + 1}"
      val metadata = Map(
        "chunk_id" -> chunkId,
        "position" -> (index + 1).toString,
        "total_chunks" -> chunks.length.toString
      )
      (chunkId, chunk, metadata)
    }
  }

  /**
   * Calculate statistical information about chunks
   * @param chunks Vector of text chunks
   * @return Map containing statistical information
   */
  def calculateChunkStatistics(chunks: Vector[PdfChunk]): Map[String, Any] = {
    if (chunks.isEmpty) {
      Map("count" -> 0)
    } else {
      val lengths = chunks.map(_.text.length)
      Map(
        "count" -> chunks.length,
        "min_length" -> lengths.min,
        "max_length" -> lengths.max,
        "avg_length" -> lengths.sum / chunks.length.toDouble,
        "total_characters" -> lengths.sum
      )
    }
  }

  /**
   * Main method to test the chunker functionality
   * @param args Command line arguments (not used)
   */
  def main(args: Array[String]): Unit = {
    // Default test PDF path - modify this to your actual test PDF file
    val testPdfPath = "/Users/mehulmathur/UIC/Cloud/Project/cs441-hw2-spark-live-rag/data/text_corpus/1083142.1083143.pdf"
    val logFilePath = "/Users/mehulmathur/UIC/Cloud/Project/cs441-hw2-spark-live-rag/data/chunker_test_log.txt"

    // Create log file writer
    val logFile = new File(logFilePath)
    val logWriter = new PrintWriter(logFile)

    try {
      logWriter.println(s"Testing PdfChunker with file: $testPdfPath")
      logWriter.println(s"Timestamp: ${java.time.LocalDateTime.now()}")
      logWriter.println("=" * 80)

      // Also print to console for immediate feedback
      println(s"Testing PdfChunker with file: $testPdfPath")
      println(s"Logging results to: $logFilePath")

      // Test with different window sizes and overlap configurations
      val configurations = Seq(
        (1000, 200, "Small window, 20% overlap"),
        (1500, 250, "Medium window, ~17% overlap"),
        (2000, 400, "Large window, 20% overlap")
      )

      for ((windowSize, overlapSize, description) <- configurations) {
        logWriter.println(s"\nTesting configuration: $description")
        logWriter.println(s"Window size: $windowSize, Overlap: $overlapSize")

        // Process the PDF file
        chunkPdfFile(testPdfPath, windowSize, overlapSize) match {
          case Some(chunks) =>
            // Log statistics
            val stats = calculateChunkStatistics(chunks)
            logWriter.println("Chunking results:")
            logWriter.println(s"  Total chunks: ${stats("count")}")
            logWriter.println(s"  Min chunk length: ${stats("min_length")}")
            logWriter.println(s"  Max chunk length: ${stats("max_length")}")
            logWriter.println(s"  Average chunk length: ${stats("avg_length").asInstanceOf[Double].round}")
            logWriter.println(s"  Total characters: ${stats("total_characters")}")

            // Log preview of first few chunks
            logWriter.println("\nPreview of first 2 chunks:")
            chunks.take(2).zipWithIndex.foreach { case (chunk, i) =>
              logWriter.println(s"  Chunk ${i+1} (${chunk.text.length} chars):")
              logWriter.println(s"  Start and End indexes: ${chunk.startIndex} - ${chunk.endIndex}")
              logWriter.println(s"  ${chunk.text.take(150)}...")
            }

            // Log preview of last chunk
            logWriter.println("\nPreview of last chunk:")
            chunks.lastOption.foreach { chunk =>
              logWriter.println(s"  Last chunk (${chunk.text.length} chars):")
              logWriter.println(s"  Start and End indexes: ${chunk.startIndex} - ${chunk.endIndex}")
              logWriter.println(s"  ${chunk.text.take(150)}...")
            }

            // Also log a sample of all chunks (first 100 characters)
            logWriter.println("\nAll chunks:")
            chunks.zipWithIndex.foreach { case (chunk, i) =>
              logWriter.println(s"  Chunk ${i+1} (${chunk.text.length} chars) Start = ${chunk.startIndex}, End = ${chunk.endIndex}: ${chunk.text}...")
            }

            // Print console feedback
            println(s"  Configuration: $description - Created ${stats("count")} chunks")

          case None =>
            logWriter.println("Failed to extract text from the PDF file.")
            println("Failed to extract text from the PDF file.")
        }

        logWriter.println("\n" + "="*80)
      }

      println(s"Chunking complete. Results logged to: $logFilePath")

    } finally {
      // Make sure to close the writer
      logWriter.close()
    }
  }
}