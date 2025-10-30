package config

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

case class SparkConfig(
  appName: String,
  master: String,
  warehouseDir: String,
  extensions: String,
  catalog: String
)

case class DataConfig(
  sourceFolder: String,
  downloadedPdfsDir: String,
  snapshotsBasePath: String
)

case class EmbeddingConfig(
  modelName: String,
  modelVersion: String,
  batchSize: Int,
  ollamaHost: String,
  ollamaPort: Int,
  timeoutSeconds: Int
)

case class AppConfig(
  spark: SparkConfig,
  data: DataConfig,
  embedding: EmbeddingConfig
)

object AppConfig {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def load(env: String = "local"): AppConfig = {
    val config = ConfigFactory.load()

    logger.info(s"Loading configuration for environment: $env")

    val sparkConfig = SparkConfig(
      appName = config.getString("app.name"),
      master = getConfigValue(config, env, "spark.master"),
      warehouseDir = getConfigValue(config, env, "spark.warehouse.dir"),
      extensions = config.getString("spark.extensions"),
      catalog = config.getString("spark.catalog")
    )

    val dataConfig = DataConfig(
      sourceFolder = getConfigValue(config, env, "data.source.folder"),
      downloadedPdfsDir = config.getString("data.downloaded.pdfs.dir"),
      snapshotsBasePath = getConfigValue(config, env, "data.snapshots.base.path")
    )

    val embeddingConfig = EmbeddingConfig(
      modelName = config.getString("embedding.model.name"),
      modelVersion = config.getString("embedding.model.version"),
      batchSize = config.getInt("embedding.batch.size"),
      ollamaHost = config.getString("embedding.ollama.host"),
      ollamaPort = config.getInt("embedding.ollama.port"),
      timeoutSeconds = config.getInt("embedding.ollama.timeout.seconds")
    )

    AppConfig(sparkConfig, dataConfig, embeddingConfig)
  }

  /**
   * Gets configuration value, first trying environment-specific path, then falling back to default path
   */
  private def getConfigValue(config: Config, env: String, path: String): String = {
    val envPath = s"environments.$env.$path"
    logger.info(s"Using environment-specific config: $envPath")

    if (config.hasPath(envPath)) {
      val value = config.getString(envPath)
      logger.debug(s"Using environment-specific config: $envPath = $value")
      value
    } else {
      val value = config.getString(path)
      logger.debug(s"Using default config: $path = $value")
      value
    }
  }
}
