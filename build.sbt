ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.15"
ThisBuild / useCoursier := false

lazy val root = (project in file("."))
  .settings(
    name := "SparkStreaming",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql" % "3.5.1",
      "org.apache.spark" %% "spark-streaming" % "3.5.1",
      "org.apache.hadoop" % "hadoop-common" % "3.2.0",
      "org.json4s" %% "json4s-native" % "3.6.11",
      "org.json4s" %% "json4s-core" % "3.6.11",
      "org.json4s" %% "json4s-jackson" % "3.6.11",
      "org.apache.hadoop" % "hadoop-hdfs" % "3.2.0",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1",
      "org.apache.kafka" % "kafka-clients" % "2.8.0",
      "com.typesafe.play" %% "play-json" % "2.9.2",
      "com.opencsv" % "opencsv" % "5.5.2",
      "io.delta" %% "delta-spark" % "3.2.0",
      "com.typesafe.akka" %% "akka-http" % "10.5.3",
      "com.typesafe.akka" %% "akka-stream" % "2.8.6"


    ),
    Compile / packageBin / mainClass := Some("Consumer"),
    updateOptions := updateOptions.value.withCachedResolution(true)
  )