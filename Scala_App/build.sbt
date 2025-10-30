ThisBuild / version := "0.1.0-SNAPSHOT"

// Check for EMR build argument
val isEmrBuild = sys.props.get("emr").isDefined || sys.env.get("EMR_BUILD").isDefined

// Conditionally set Scala version based on build type
ThisBuild / scalaVersion := {
  if (isEmrBuild) {
    println("🚀 Building for EMR: Using Scala 2.12.18 + Spark 3.5.5 (provided)")
    "2.12.18"  // EMR uses Scala 2.12
  } else {
    println("🏠 Building for Local: Using Scala 2.13.16 + Spark 4.0.1")
    "2.13.16"  // Local development uses Scala 2.13
  }
}

ThisBuild / javaHome := Some(file("/Library/Java/JavaVirtualMachines/openjdk-17.jdk/Contents/Home"))

// Force 'sbt run' to start your application in a new JVM
fork := true

// Add JVM options including Hadoop configuration
javaOptions ++= Seq(
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "-Dhadoop.home.dir=/opt/homebrew/opt/hadoop/",
  "-Djava.net.preferIPv4Stack=true",
  "-Dhadoop.conf.dir=/opt/homebrew/opt/hadoop/libexec/etc/hadoop/"
)

// Add Hadoop configuration directory to resources
Compile / unmanagedResourceDirectories += file("/opt/homebrew/opt/hadoop/libexec/etc/hadoop")

// Library dependencies with conditional scoping
libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.5.19",
  "org.slf4j" % "slf4j-api" % "2.0.17",
  "org.apache.lucene" % "lucene-analysis-common" % "10.3.1",
  "org.apache.lucene" % "lucene-backward-codecs" % "10.3.1",
  "org.apache.lucene" % "lucene-queryparser" % "10.3.1",

  // PDF extraction + HTTP + JSON
  "org.apache.pdfbox" % "pdfbox" % "3.0.5",
  "org.apache.tika" % "tika-core" % "3.2.3",
  "org.apache.tika" % "tika-parser-pdf-module" % "3.2.3",

  // ML and language detection
  "org.apache.opennlp" % "opennlp-tools" % "2.5.6",

  // HTTP client and JSON parsing
  "com.softwaremill.sttp.client3" %% "core" % "3.11.0",
  "com.softwaremill.sttp.client3" %% "circe" % "3.11.0",
  "io.circe" %% "circe-generic" % "0.14.15",
  "io.circe" %% "circe-parser" % "0.14.15",
  "io.cequence" %% "openai-scala-client" % "1.2.0",
  "org.json4s" %% "json4s-native" % "4.0.7",

  // AWS SDK for S3 integration
  "software.amazon.awssdk" % "s3" % "2.35.8",
  "software.amazon.awssdk" % "auth" % "2.35.8",
  "software.amazon.awssdk" % "regions" % "2.35.8",

  // Typesafe Config
  "com.typesafe" % "config" % "1.4.5",

  // Testing dependencies
  "org.scalatestplus" %% "mockito-5-18" % "3.2.19.0" % "test",
  "org.scalatest" %% "scalatest" % "3.2.19" % "test"
) ++ (
  // Conditional Spark/Hadoop dependencies based on build type
  if (isEmrBuild) {
    Seq(
      // EMR: Spark 3.5.5 + Scala 2.12 (provided by EMR)
      "org.apache.spark" %% "spark-core" % "3.5.5" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.5.5" % "provided",
      "io.delta" %% "delta-spark" % "3.3.1" % "provided",
      "org.apache.spark" %% "spark-hive" % "3.5.5" % "provided",
      "org.apache.hadoop" % "hadoop-client" % "3.4.1" % "provided"
    )
  } else {
    Seq(
      // Local: Spark 4.0.1 + Scala 2.13 (included in JAR)
      "org.apache.spark" %% "spark-core" % "4.0.1",
      "org.apache.spark" %% "spark-sql" % "4.0.1",
      "io.delta" %% "delta-spark" % "4.0.0",
      "org.apache.spark" %% "spark-hive" % "4.0.1",
      "org.apache.hadoop" % "hadoop-client" % "3.4.1"
    )
  }
)

// Fix for Scala 2.13 compatibility issues
dependencyOverrides += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.4.0"

// Assembly merge strategy configuration
ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", "resources", xs @ _*) => MergeStrategy.concat
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) if xs.nonEmpty && (xs.last.endsWith(".properties") || xs.last.endsWith(".types")) => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("reference.conf") => MergeStrategy.concat
  case PathList("application.conf") => MergeStrategy.concat
  case PathList("log4j.properties") => MergeStrategy.first
  case PathList("logback.xml") => MergeStrategy.first
  // Hive and DataNucleus specific merge strategies
  case PathList("org", "datanucleus", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "hadoop", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "spark", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "hive", xs @ _*) => MergeStrategy.first
  case PathList("com", "google", xs @ _*) => MergeStrategy.first
  case PathList("javax", xs @ _*) => MergeStrategy.first
  // DataNucleus plugin files
  case x if x.endsWith("plugin.xml") => MergeStrategy.concat
  case x if x.endsWith("PLUGIN.xml") => MergeStrategy.concat
  case x if x.contains("datanucleus") => MergeStrategy.first
  case x if x.endsWith(".class") => MergeStrategy.first
  case x if x.endsWith(".proto") => MergeStrategy.first
  case x if x.contains("module-info") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

// Project configuration
lazy val root = (project in file("."))
  .settings(
    name := "Scala_App",
    // Set the main class for assembly and regular builds
    assembly / mainClass := Some("DeltaIndexerDriver"),
    Compile / mainClass := Some("DeltaIndexerDriver"),
    // Conditional assembly JAR name based on build type
    assembly / assemblyJarName := {
      if (isEmrBuild) "rag-delta-indexer-emr-assembly.jar"
      else "rag-delta-indexer-local-assembly.jar"
    },
    // Set assembly log level
    assembly / logLevel := Level.Info
  )
