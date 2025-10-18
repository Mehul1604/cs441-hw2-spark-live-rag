import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object DeltaIndexerDriver {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Starting RAG Delta Indexer application")

    try {
      val spark = SparkSession.builder
        .appName("RAG Delta Indexer")
        .master("local[*]") // change later when deploying to AWS EMR - will change to yarn or EMR default
        .getOrCreate()

      logger.info("Spark session created successfully")

      // Your Delta indexing logic goes here

      // read the source folder containing the pdf files
  //    val sourceFolder = "s3://your-bucket/path/to/pdf/files/"
      // local path for now
      val sourceFolder = "/Users/mehulmathur/UIC/Cloud/Project/cs441-hw2-spark-live-rag/data/text_corpus"
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

      logger.info("Shutting down Spark session")
      spark.stop()
      logger.info("Application completed successfully")
    } catch {
      case e: Exception =>
        logger.error(s"An error occurred during execution: ${e.getMessage}", e)
        throw e
    }
  }
}