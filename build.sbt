import Dependencies._

val buildResolvers = Seq(
    "Local Maven Repository"    at "file://"+Path.userHome.absolutePath+"/.m2/repository",
    "Maven repository"          at "https://download.java.net/maven/2/",
    "Typesafe Repo"             at "https://repo.typesafe.com/typesafe/releases/",
    "Sonatype Snapshots"        at "https://oss.sonatype.org/content/repositories/snapshots",
    "Sonatype Releases"         at "https://oss.sonatype.org/content/repositories/releases"
  )

lazy val root = (project in file("."))
  .settings(
    inThisBuild(List(
      organization := "io.opentargets",
      scalaVersion := "2.12.12",
      version      := "latest"
    )),
    name := "ot-geckopipe",
    resolvers ++= buildResolvers,

    // from Dependencies.scala
    libraryDependencies += scalaCheck,
    libraryDependencies ++= configDeps,
    libraryDependencies ++= loggingDeps,
    libraryDependencies ++= scalaMiniTestSeq,
    libraryDependencies ++= codeDeps,
    libraryDependencies ++= sparkDeps,

    testFrameworks += new TestFramework("minitest.runner.Framework"),

    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") =>
        MergeStrategy.filterDistinctLines
      case PathList("META-INF", "services", "org.apache.spark.sql.sources.DataSourceRegister") =>
        MergeStrategy.concat
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )
