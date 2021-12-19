ThisBuild / organization := "io.github.jasonheo"
ThisBuild / version := "0.1"
ThisBuild / scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.12" % "3.0.5" % "test",
  "io.github.embeddedkafka" %% "embedded-kafka" % "2.6.0" % "test"
)
