import Dependencies._

ThisBuild / scalaVersion     := "2.12.15"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "cryptoanalyse",
    libraryDependencies += munit % Test
  )
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.4",
  "org.apache.spark" %% "spark-sql" % "3.3.4",
)
// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
