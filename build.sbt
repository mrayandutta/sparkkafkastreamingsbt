name := "sparkkafkastreamingsbt"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion="1.6.3"
val log4jVersion="1.2.14"
val kafkaVersion="0.8.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.0",
  "org.apache.spark" %% "spark-streaming" % "2.0.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.0.0",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

