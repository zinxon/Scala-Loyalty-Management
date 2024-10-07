ThisBuild / scalaVersion := "2.13.15"

lazy val root = (project in file("."))
  .settings(
    name := "KafkaIOStream"
  )
ThisBuild / version := "0.1.0-SNAPSHOT"

val sparkVersion = "3.5.0"
libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" %sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "log4j" % "log4j" % "1.2.17",
  "com.typesafe" % "config" % "1.4.2"  // to read configurations from properties file
)