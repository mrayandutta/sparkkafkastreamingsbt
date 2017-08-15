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
  "log4j" % "log4j" % "1.2.14",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "1.6.0_0.3.1"
  //"com.holdenkarau" % "spark-testing-base_2.11" % "1.6.3_0.7.3"
  //"com.holdenkarau" % "spark-testing-base_2.11" % "1.5.1_0.2.1" % "test"

)

