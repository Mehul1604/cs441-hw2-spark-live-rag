ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.16"

ThisBuild / javaHome := Some(file("/Library/Java/JavaVirtualMachines/openjdk-17.jdk/Contents/Home"))

// 1. Force 'sbt run' to start your application in a new JVM
fork := true
// 2. Add the --add-opens flags to the new JVM
javaOptions ++= Seq(
  "--add-opens=java.base/java.net=ALL-UNNAMED"
)

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

  // Spark
  "org.apache.spark" %% "spark-core" % "4.0.1",
  "org.apache.spark" %% "spark-sql" % "4.0.1",
  "io.delta" %% "delta-spark" % "4.0.0",
  "org.apache.spark" %% "spark-hive" % "4.0.1", // Added Hive support

  // ML and language detection
  "org.apache.opennlp" % "opennlp-tools" % "2.5.6",

  "org.apache.hadoop" % "hadoop-client" % "3.4.1",
//  "org.apache.hadoop" % "hadoop-aws" % "3.4.2",

  // HTTP client and JSON parsing
  "com.softwaremill.sttp.client3" %% "core"  % "3.11.0",
  "com.softwaremill.sttp.client3" %% "circe" % "3.11.0",
  "io.circe" %% "circe-generic" % "0.14.15",
  "io.circe" %% "circe-parser"  % "0.14.15",
  "io.cequence" %% "openai-scala-client" % "1.2.0",
  "org.json4s" %% "json4s-native" % "4.0.7",
//
  // AWS SDK for S3 integration
  "software.amazon.awssdk" % "s3" % "2.35.8",
  "software.amazon.awssdk" % "auth" % "2.35.8",
  "software.amazon.awssdk" % "regions" % "2.35.8",

  // Typesafe Config
  "com.typesafe" % "config" % "1.4.5",

  // Testing dependencies
  "org.scalatestplus" %% "mockito-5-18" % "3.2.19.0" % "test",
  "org.scalatest" %% "scalatest" % "3.2.19" % "test"

)

// Fix for Scala 2.13 compatibility issues
dependencyOverrides += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.4.0"
// Add Janino dependency for logback filtering
libraryDependencies += "org.codehaus.janino" % "janino" % "3.1.12"


lazy val root = (project in file("."))
  .settings(
    name := "Scala_App"
  )
