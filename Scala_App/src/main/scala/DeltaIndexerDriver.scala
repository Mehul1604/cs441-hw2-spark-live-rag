import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.slf4j.LoggerFactory

import java.io.File
import utils.PdfTextExtractor.extractText
import utils.PdfChunker.{PdfChunk, createChunks}
import utils.LanguageDetector.{LanguageResult, SerializableLanguageDetector}
import akka.actor.ActorSystem

import scala.concurrent.ExecutionContext
import llm.{Embeddings, OllamaClient, SerializableEmbeddingGenerator}

object DeltaIndexerDriver {
  // Case class for chunk data - must be defined at the top level for serialization
  case class ChunkData(idx: Int, startIndex: Int, endIndex: Int, text: String)

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Starting RAG Delta Indexer application")

    // one arg/flag - --reindex - if this is an fresh reindex run deleting tables - if not then false


    // parse --incr flag
    val reindex: Boolean = args.contains("--reindex")

    try {
      val spark = SparkSession.builder
        .appName("RAG Delta Indexer")
        .master("local[*]") // change later when deploying to AWS EMR - will change to yarn or EMR default
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "/Users/mehulmathur/UIC/Cloud/Project/cs441-hw2-spark-live-rag/Scala_App/spark-warehouse")
        .enableHiveSupport()
        .getOrCreate()

//      import spark.implicits._

      if(!reindex) {
        logger.info("Running in incremental mode")

        logger.info("Ensuring 'rag' schema exists")
        spark.sql("CREATE DATABASE IF NOT EXISTS rag")


        logger.info("Available databases in the catalog:")
        spark.sql("SHOW DATABASES").show(false)

        // list tables in rag schema
        logger.info("Tables in 'rag' schema:")
        spark.sql("SHOW TABLES IN rag").show(false)
      } else {
        logger.info("Running in full reindex mode")

        // Delete and recreate rag schema
        logger.info("Dropping and recreating 'rag' schema for full reindex")
        spark.sql("DROP DATABASE IF EXISTS rag CASCADE")
        spark.sql("CREATE DATABASE rag")

        logger.info("Available databases in the catalog after recreation:")
        spark.sql("SHOW DATABASES").show(false)
      }


      logger.info("Spark session created successfully")

      // local path for now
      val sourceFolder = "/Users/mehulmathur/UIC/Cloud/Project/cs441-hw2-spark-live-rag/data/text_corpus_small"
      logger.info(s"Reading PDF files from: $sourceFolder")

      val rawPdfs = spark.read.format("binaryFile")
        .option("pathGlobFilter", "*.pdf")
        .load(sourceFolder)

      // show the schema of the loaded data
      logger.info("PDF data schema:")
      rawPdfs.printSchema()

      // Count and log the number of PDFs loaded
      val pdfCount = rawPdfs.count()
      logger.info(s"Loaded $pdfCount PDF files from $sourceFolder")

      // Test language detector standalone before creating UDF
      val langDetectorService = new SerializableLanguageDetector()
      val testText = "This is a sample English text."
      val testResult = langDetectorService.detectLanguage(testText)
      logger.info(s"Language detection test: Detected language for sample text is '${testResult.lang}'")

      // Broadcast the detector service to all workers
      val langDetectorBroadcast = spark.sparkContext.broadcast(langDetectorService)

      // Further Processing to get the text and other metadata
      val getTextUdf = udf((fileUri: String) => extractText(fileUri.replace("file:", "")) match {
        case Some(text) => text
        case None => ""
      })

      // Create a UDF that uses the broadcast variable
      val detectLanguageUdf = udf((text: String) => {
        if (text == null || text.isEmpty) {
          "unknown"
        } else {
          langDetectorBroadcast.value.detectLanguageWithSampling(text).lang
        }
      })

      val pdfDocs = rawPdfs.select(
        col("path").as("pdfUri"),
        getTextUdf(col("pdfUri")).as("text")
      )
        .withColumn("language", detectLanguageUdf(col("text")))
        // get the first line of the text as title by getting substring till first new line
        .withColumn("title", trim(expr("substring_index(text, '\n', 1)")))
        // get the docId by sha2 hashing the pdfUri
        .withColumn("docId", sha2(col("pdfUri"), 256))
        // get the content hash by sha2 hashing the text
        .withColumn("contentHash", sha2(col("text"), 256))
        .filter(col("text") =!= "")
        .cache()

      // display the schema of the processed pdf documents
      logger.info("Processed PDF documents schema:")
      pdfDocs.printSchema()

      // Uncomment to log sample processed documents
//      logger.info("Sample processed PDF documents (first 5):")
//      val sampleDocs = pdfDocs
//        .select("pdfUri", "title", "language", "docId", "contentHash")
//        .limit(5)
//        .collect()
//
//      // Log each sample document
//      sampleDocs.foreach { row =>
//        val uri = row.getAs[String]("pdfUri")
//        val title = row.getAs[String]("title")
//        val lang = row.getAs[String]("language")
//        val docId = row.getAs[String]("docId").take(10) + "..." // Truncate for readability
//
//        logger.info(s"PDF: ${new File(uri).getName}, Title: ${title.take(50)}${if (title.length > 50) "..." else ""}, Lang: $lang, DocId: $docId")
//      }

      // -- Delta detection

      val docsToProcess = if (spark.catalog.tableExists("rag", "delta_pdf_docs")) {
        logger.info("Delta table exists. Performing delta detection.")
        val existingDocs = spark.table("rag.delta_pdf_docs")
          .select("docId", "contentHash")
          .distinct()

        val deltaDocs = pdfDocs.alias("new_docs")
          .join(existingDocs.alias("existing_docs"), Seq("docId", "contentHash"), "left_anti")

        deltaDocs
      } else {
        logger.info("Delta table does not exist. All documents will be processed.")
        pdfDocs
      }

      logger.info(s"New or changed documents in this run: ${docsToProcess.count()}")

      // -- Chunking
      val chunkUdf = udf((text: String) => {
        if (text == null || text.isEmpty) {
          Array.empty[ChunkData]
        } else {
          createChunks(text).zipWithIndex.map { case (chunk, idx) =>
            ChunkData(idx, chunk.startIndex, chunk.endIndex, chunk.text)
          }.toArray
        }
      })

      val chunkedDocs = docsToProcess
        .withColumn("chunk", explode(chunkUdf(col("text"))))
        .select(
          col("docId"),
          col("chunk.idx").as("chunkIndex"),
          col("chunk.startIndex").as("chunkStartIndex"),
          col("chunk.endIndex").as("chunkEndIndex"),
          col("chunk.text").as("chunkText"),
          col("pdfUri").as("sectionPath")
        )
        .withColumn("chunkContentHash", sha2(col("chunkText"), 256))
        .withColumn("chunkId",
          sha2(concat_ws(":",
            col("docId"),
            col("chunkIndex"),
            col("chunkStartIndex"),
            col("chunkEndIndex")),
          256))

        .cache()

      logger.info(s"Total chunks to process: ${chunkedDocs.count()}")
      // Show schema of chunked documents
      logger.info("Chunked documents schema:")
      chunkedDocs.printSchema()

      // Uncomment to log sample chunked documents
//      logger.info("Sample chunked documents (first 5):")
//      val sampleChunks = chunkedDocs
//        .select("docId", "chunkIndex", "chunkStartIndex", "chunkEndIndex", "chunkText", "sectionPath", "chunkId")
//        .limit(5)
//        .collect()
//      // Log each sample chunk
//      sampleChunks.foreach { row =>
//        val docId = row.getAs[String]("docId").take(10) + "..."
//        val chunkIndex = row.getAs[Int]("chunkIndex")
//        val chunkStart = row.getAs[Int]("chunkStartIndex")
//        val chunkEnd = row.getAs[Int]("chunkEndIndex")
//        val chunkText = row.getAs[String]("chunkText")
//        val sectionPath = row.getAs[String]("sectionPath")
//        val chunkId = row.getAs[String]("chunkId").take(10) + "..."
//
//        logger.info(s"DocId: $docId, ChunkIndex: $chunkIndex, Start: $chunkStart, End: $chunkEnd, SectionPath: ${new File(sectionPath).getName}, ChunkId: $chunkId, ChunkText: ${chunkText.take(50)}${if (chunkText.length > 50) "..." else ""}")
//      }

      // -- Upsert docs/chunks into Delta tables

      // temp views
      docsToProcess.createOrReplaceTempView("staging_pdf_docs")
      chunkedDocs.createOrReplaceTempView("staging_pdf_chunks")

      // rag.delta_pdf_docs table
      logger.info("Upserting documents into Delta table: rag.delta_pdf_docs")
      if(!spark.catalog.tableExists("rag", "delta_pdf_docs")) {  // Fixed tableExists call
        logger.info("Delta table rag.delta_pdf_docs does not exist. Creating new table.")
        docsToProcess.write
          .format("delta")
          .mode("overwrite")
          .saveAsTable("rag.delta_pdf_docs")
      } else {
        logger.info("Delta table rag.delta_pdf_docs exists. Merging new documents.")
        spark.sql(
          """
            |MERGE INTO rag.delta_pdf_docs target
            |USING staging_pdf_docs source
            |ON target.docId = source.docId
            |WHEN MATCHED THEN UPDATE SET *
            |WHEN NOT MATCHED THEN INSERT *
            |""".stripMargin)
      }

      // rag.delta_pdf_chunks table
      logger.info("Upserting chunks into Delta table: rag.delta_pdf_chunks")
      if(!spark.catalog.tableExists("rag", "delta_pdf_chunks")) {  // Fixed tableExists call
        logger.info("Delta table rag.delta_pdf_chunks does not exist. Creating new table.")
        chunkedDocs.write
          .format("delta")
          .mode("overwrite")
          .saveAsTable("rag.delta_pdf_chunks")
      } else {
        logger.info("Delta table rag.delta_pdf_chunks exists. Merging new chunks.")
        spark.sql(
          """
            |MERGE INTO rag.delta_pdf_chunks target
            |USING staging_pdf_chunks source
            |ON target.chunkId = source.chunkId
            |WHEN MATCHED THEN UPDATE SET *
            |WHEN NOT MATCHED THEN INSERT *
            |""".stripMargin)
      }

      // Show the delta tables count and sample 5 rows
      val finalDocCount = spark.table("rag.delta_pdf_docs").count()
      val finalChunkCount = spark.table("rag.delta_pdf_chunks").count()
      logger.info(s"Total documents in rag.delta_pdf_docs: $finalDocCount")
      logger.info(s"Total chunks in rag.delta_pdf_chunks: $finalChunkCount")
      // Uncomment to show sample rows
//      logger.info("Sample rows from rag.delta_pdf_docs:")
//      spark.table("rag.delta_pdf_docs").show(5, truncate = true)
//
//      logger.info("Sample rows from rag.delta_pdf_chunks:")
//      spark.table("rag.delta_pdf_chunks").show(5, truncate = true)


      // -- Embedding the delta chunks
      logger.info("Embedding new chunks from rag.delta_pdf_chunks")

      val modelName = "mxbai-embed-large"
      val modelVersion = "v1"

      // Create a serializable embedding generator that will be recreated on each executor
      // to avoid serialization issues with ActorSystem and other non-serializable components
      val embeddingGeneratorCreator = new SerializableEmbeddingGenerator(modelName, modelVersion)
      logger.info(s"Created serializable embedding generator for model: $modelName v$modelVersion")

      val chunksToEmbed = if(spark.catalog.tableExists("rag", "delta_embeddings")) {
        logger.info("Embedding delta table exists. Performing delta detection for embeddings.")
        val existingEmbeddings = spark.table("rag.delta_embeddings")
          .where(col("modelName") === lit(modelName) && col("modelVersion") === lit(modelVersion))
          .select("chunkId", "chunkContentHash", "chunkText")

        val deltaChunks = chunkedDocs.alias("new_chunks")
          .join(existingEmbeddings.alias("existing_embeddings"), Seq("chunkId", "chunkContentHash"), "left_anti")

        deltaChunks
      } else {
        logger.info("Embedding delta table does not exist. All chunks will be embedded.")
        chunkedDocs
          .select("chunkId", "chunkContentHash", "chunkText")
      }


      logger.info("New or changed chunks to embed: " + chunksToEmbed.count())

      // Since we have a batch DataFrame, not a streaming one, process it directly
      logger.info("Starting embedding process for chunks in batch mode")

      // Use the same batch processing logic but call it directly
      import spark.implicits._
      chunksToEmbed
        .as[(String, String, String)]
        .mapPartitions { rows =>
          val batchedRows = rows.grouped(10).flatMap { batch =>
            val texts = batch.map(_._3).toVector
            val embeddings = embeddingGeneratorCreator.generateEmbeddings(texts)
            batch.zip(embeddings).map { case ((chunkId, chunkContentHash, _), embedding) =>
              (chunkId, chunkContentHash, modelName, modelVersion, embedding)
            }
          }
          batchedRows
        }
        .toDF("chunkId", "chunkContentHash", "modelName", "modelVersion", "embedding")
        .write.format("delta").mode("append")
        .saveAsTable("rag.delta_embeddings")

      logger.info("Embedding process completed for chunks")


      // show sample 5 rows
      logger.info("Sample rows from rag.delta_embeddings:")
      spark.table("rag.delta_embeddings").show(5, truncate = true)


      logger.info("Shutting down Spark session")
      spark.stop()
      logger.info("Application completed successfully")
    } catch {
      case e: Exception =>
        logger.error(s"An error occurred during execution: ${e.getMessage}", e)
        e.printStackTrace()
        throw e
    }
  }
}