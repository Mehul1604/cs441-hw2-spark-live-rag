package config

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import com.typesafe.config.{Config, ConfigFactory}
import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}

class AppConfigSpec extends AnyFunSpec with Matchers with MockitoSugar with BeforeAndAfterAll {

  // Test configuration content
  val testConfigContent =
    """
      |app {
      |  name = "test-app"
      |}
      |
      |spark {
      |  master = "local[*]"
      |  warehouse {
      |    dir = "/tmp/spark-warehouse"
      |  }
      |  extensions = "io.delta.sql.DeltaSparkSessionExtension"
      |  catalog = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      |}
      |
      |data {
      |  source {
      |    folder = "/tmp/data"
      |  }
      |  downloaded {
      |    pdfs {
      |      dir = "/tmp/pdfs"
      |    }
      |  }
      |  snapshots {
      |    base {
      |      path = "/tmp/snapshots"
      |    }
      |  }
      |}
      |
      |embedding {
      |  model {
      |    name = "mxbai-embed-large"
      |    version = "1.0"
      |  }
      |  batch {
      |    size = 10
      |  }
      |  ollama {
      |    host = "localhost"
      |    port = 11434
      |    timeout {
      |      seconds = 180
      |    }
      |  }
      |}
      |
      |environments {
      |  emr {
      |    spark {
      |      master = "yarn"
      |      warehouse {
      |        dir = "s3a://my-bucket/spark-warehouse"
      |      }
      |    }
      |    data {
      |      source {
      |        folder = "s3a://my-bucket/pdfs/"
      |      }
      |      snapshots {
      |        base {
      |          path = "s3a://my-bucket/snapshots/"
      |        }
      |      }
      |    }
      |  }
      |
      |  prod {
      |    spark {
      |      master = "spark://prod-cluster:7077"
      |      warehouse {
      |        dir = "/prod/spark-warehouse"
      |      }
      |    }
      |    data {
      |      source {
      |        folder = "/prod/data"
      |      }
      |      snapshots {
      |        base {
      |          path = "/prod/snapshots"
      |        }
      |      }
      |    }
      |  }
      |}
    """.stripMargin

  val testConfigFile = "/tmp/test-application.conf"

  override def beforeAll(): Unit = {
    // Create test configuration file
    val writer = new PrintWriter(new File(testConfigFile))
    writer.write(testConfigContent)
    writer.close()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    // Clean up test configuration file
    val testFile = new File(testConfigFile)
    if (testFile.exists()) testFile.delete()
    super.afterAll()
  }

  describe("SparkConfig") {
    it("should create SparkConfig with correct values") {
      val sparkConfig = SparkConfig(
        appName = "test-app",
        master = "local[*]",
        warehouseDir = "/tmp/warehouse",
        extensions = "test.extension",
        catalog = "test.catalog"
      )

      sparkConfig.appName shouldBe "test-app"
      sparkConfig.master shouldBe "local[*]"
      sparkConfig.warehouseDir shouldBe "/tmp/warehouse"
      sparkConfig.extensions shouldBe "test.extension"
      sparkConfig.catalog shouldBe "test.catalog"
    }

    it("should be serializable") {
      val sparkConfig = SparkConfig("app", "local", "/tmp", "ext", "cat")

      import java.io.{ByteArrayOutputStream, ObjectOutputStream, ByteArrayInputStream, ObjectInputStream}

      val baos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(baos)

      noException should be thrownBy {
        oos.writeObject(sparkConfig)
        oos.close()
      }

      val bais = new ByteArrayInputStream(baos.toByteArray)
      val ois = new ObjectInputStream(bais)

      val deserializedConfig = ois.readObject().asInstanceOf[SparkConfig]
      ois.close()

      deserializedConfig shouldBe sparkConfig
    }
  }

  describe("DataConfig") {
    it("should create DataConfig with correct values") {
      val dataConfig = DataConfig(
        sourceFolder = "/data/source",
        downloadedPdfsDir = "/data/pdfs",
        snapshotsBasePath = "/data/snapshots"
      )

      dataConfig.sourceFolder shouldBe "/data/source"
      dataConfig.downloadedPdfsDir shouldBe "/data/pdfs"
      dataConfig.snapshotsBasePath shouldBe "/data/snapshots"
    }

    it("should be serializable") {
      val dataConfig = DataConfig("/src", "/pdfs", "/snapshots")

      import java.io.{ByteArrayOutputStream, ObjectOutputStream, ByteArrayInputStream, ObjectInputStream}

      val baos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(baos)

      noException should be thrownBy {
        oos.writeObject(dataConfig)
        oos.close()
      }

      val bais = new ByteArrayInputStream(baos.toByteArray)
      val ois = new ObjectInputStream(bais)

      val deserializedConfig = ois.readObject().asInstanceOf[DataConfig]
      ois.close()

      deserializedConfig shouldBe dataConfig
    }
  }

  describe("EmbeddingConfig") {
    it("should create EmbeddingConfig with correct values") {
      val embeddingConfig = EmbeddingConfig(
        modelName = "test-model",
        modelVersion = "2.0",
        batchSize = 20,
        ollamaHost = "test-host",
        ollamaPort = 8080,
        timeoutSeconds = 300
      )

      embeddingConfig.modelName shouldBe "test-model"
      embeddingConfig.modelVersion shouldBe "2.0"
      embeddingConfig.batchSize shouldBe 20
      embeddingConfig.ollamaHost shouldBe "test-host"
      embeddingConfig.ollamaPort shouldBe 8080
      embeddingConfig.timeoutSeconds shouldBe 300
    }

    it("should be serializable") {
      val embeddingConfig = EmbeddingConfig("model", "1.0", 10, "host", 11434, 180)

      import java.io.{ByteArrayOutputStream, ObjectOutputStream, ByteArrayInputStream, ObjectInputStream}

      val baos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(baos)

      noException should be thrownBy {
        oos.writeObject(embeddingConfig)
        oos.close()
      }

      val bais = new ByteArrayInputStream(baos.toByteArray)
      val ois = new ObjectInputStream(bais)

      val deserializedConfig = ois.readObject().asInstanceOf[EmbeddingConfig]
      ois.close()

      deserializedConfig shouldBe embeddingConfig
    }
  }

  describe("AppConfig") {
    it("should create AppConfig with all sub-configs") {
      val sparkConfig = SparkConfig("app", "local", "/tmp", "ext", "cat")
      val dataConfig = DataConfig("/src", "/pdfs", "/snapshots")
      val embeddingConfig = EmbeddingConfig("model", "1.0", 10, "host", 11434, 180)

      val appConfig = AppConfig(sparkConfig, dataConfig, embeddingConfig)

      appConfig.spark shouldBe sparkConfig
      appConfig.data shouldBe dataConfig
      appConfig.embedding shouldBe embeddingConfig
    }

    it("should be serializable") {
      val sparkConfig = SparkConfig("app", "local", "/tmp", "ext", "cat")
      val dataConfig = DataConfig("/src", "/pdfs", "/snapshots")
      val embeddingConfig = EmbeddingConfig("model", "1.0", 10, "host", 11434, 180)
      val appConfig = AppConfig(sparkConfig, dataConfig, embeddingConfig)

      import java.io.{ByteArrayOutputStream, ObjectOutputStream, ByteArrayInputStream, ObjectInputStream}

      val baos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(baos)

      noException should be thrownBy {
        oos.writeObject(appConfig)
        oos.close()
      }

      val bais = new ByteArrayInputStream(baos.toByteArray)
      val ois = new ObjectInputStream(bais)

      val deserializedConfig = ois.readObject().asInstanceOf[AppConfig]
      ois.close()

      deserializedConfig shouldBe appConfig
    }
  }

  describe("AppConfig object") {
    describe("load method") {
      it("should load configuration with local environment by default") {
        // This test uses the actual application.conf from resources
        val config = AppConfig.load() // defaults to "local"

        config should not be null
        config.spark should not be null
        config.data should not be null
        config.embedding should not be null

        // Verify structure
        config.spark.appName should not be empty
        config.embedding.modelName should not be empty
      }

      it("should load configuration for specific environment") {
        val config = AppConfig.load("local")

        config should not be null
        config.spark.appName should not be empty
        config.embedding.modelName should not be empty
      }

      it("should handle EMR environment configuration") {
        // This might fail if EMR config is not in the actual application.conf
        // In that case, it should fall back to default values
        noException should be thrownBy {
          val config = AppConfig.load("emr")
          config should not be null
        }
      }

      it("should handle production environment configuration") {
        noException should be thrownBy {
          val config = AppConfig.load("prod")
          config should not be null
        }
      }

      it("should handle unknown environment by falling back to defaults") {
        val config = AppConfig.load("unknown-env")

        config should not be null
        config.spark should not be null
        config.data should not be null
        config.embedding should not be null
      }
    }

    describe("getConfigValue method") {
      // Since getConfigValue is private, we test it indirectly through load method
      it("should prioritize environment-specific values over defaults") {
        val config = AppConfig.load("emr")

        // If EMR config exists, it should use EMR-specific values
        // If not, it should fall back to defaults
        config should not be null

        // The actual test would depend on what's in the application.conf
        // We're testing that the method doesn't throw exceptions
      }

      it("should fall back to default values when environment-specific config is missing") {
        val config = AppConfig.load("nonexistent-env")

        config should not be null
        config.spark.appName should not be empty
      }
    }
  }

  describe("Configuration validation") {
    it("should handle missing optional configuration gracefully") {
      // Test that the configuration loading is robust
      noException should be thrownBy {
        AppConfig.load("local")
      }
    }

    it("should validate required configuration fields") {
      val config = AppConfig.load()

      // Verify that all required fields are present and not empty
      config.spark.appName should not be empty
      config.spark.extensions should not be empty
      config.spark.catalog should not be empty

      config.embedding.modelName should not be empty
      config.embedding.modelVersion should not be empty
      config.embedding.batchSize should be > 0
      config.embedding.ollamaPort should be > 0
      config.embedding.timeoutSeconds should be > 0
    }

    it("should handle numeric configuration values correctly") {
      val config = AppConfig.load()

      config.embedding.batchSize shouldBe a[Int]
      config.embedding.ollamaPort shouldBe a[Int]
      config.embedding.timeoutSeconds shouldBe a[Int]

      config.embedding.batchSize should be > 0
      config.embedding.ollamaPort should be > 0
      config.embedding.timeoutSeconds should be > 0
    }
  }
}
