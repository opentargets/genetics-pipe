import sbt._

object Dependencies {
  lazy val scalaLoggingDep = "ch.qos.logback" % "logback-classic" % "1.2.3"
  lazy val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
  // lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
  lazy val scalaMiniTestSeq = Seq(
    "io.monix" %% "minitest" % "2.2.1" % "test",
    "io.monix" %% "minitest-laws" % "2.2.1" % "test"
  )
  lazy val scopt = "com.github.scopt" %% "scopt" % "3.7.0"
  lazy val scalaCheck = "org.scalacheck" %% "scalacheck" % "1.14.0"
  lazy val sparkSeq = Seq(
    "org.apache.spark" %% "spark-core" % "2.4.0",
    "org.apache.spark" %% "spark-sql" % "2.4.0",
    "org.apache.spark" %% "spark-graphx" % "2.4.0",
    "org.apache.spark" %% "spark-mllib" % "2.4.0"
    )
  lazy val configLB = "com.github.pureconfig" %% "pureconfig" % "0.10.1"
}
