ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.8"
val sparkVersion = "2.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion  % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion  % "provided",
  "org.postgresql" % "postgresql" % "42.3.3"
)

lazy val root = (project in file("."))
  .settings(
    name := "test_spark"
  )
