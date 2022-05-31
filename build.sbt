import Dependencies._

val buildResolvers = Seq(
  "Maven repository" at "https://download.java.net/maven/2/",
  "Typesafe Repo" at "https://repo.typesafe.com/typesafe/releases/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases"
)

lazy val root = (project in file("."))
  .settings(
    inThisBuild(
      List(
        organization := "io.opentargets",
        scalaVersion := "2.12.12",
        version := "1.0.0"
      )),
    name := "etl-genetics",
    resolvers ++= buildResolvers,
    // from Dependencies.scala
    libraryDependencies ++= dependencies,
    testFrameworks += new TestFramework("minitest.runner.Framework"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") =>
        MergeStrategy.filterDistinctLines
      case PathList("META-INF", "services", "org.apache.spark.sql.sources.DataSourceRegister") =>
        MergeStrategy.concat
      case PathList("META-INF", _ @_*) => MergeStrategy.discard
      case _                           => MergeStrategy.first
    }
  )
