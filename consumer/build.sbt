scalaVersion := "2.12.18"
name := "f1-telemetry-consumer"
organization := "cc.phish3y.f1-telemetry-consumer"
version := "1.0"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.6" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.6" % "provided"
// libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.5.2"
