import sbt._

object Dependencies {

  lazy val dependencies
    : Seq[ModuleID] = configDeps ++ loggingDeps ++ codeDeps ++ testingDeps ++ sparkDeps ++ gcp

  lazy val configDeps = Seq(
    "com.github.pureconfig" %% "pureconfig" % "0.14.1"
  )

  lazy val loggingDeps = Seq(
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
  )

  lazy val codeDeps = Seq(
    "com.beachape" %% "enumeratum" % "1.6.1",
    "com.github.scopt" %% "scopt" % "3.7.1"
  )

  lazy val miniTestVersion = "2.9.6"
  lazy val testingDeps = Seq(
    "io.monix" %% "minitest" % miniTestVersion % "test",
    "io.monix" %% "minitest-laws" % miniTestVersion % "test",
    "org.scalacheck" %% "scalacheck" % "1.14.3"
  )

  lazy val sparkVersion = "3.1.2"
  lazy val sparkDeps = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  )
  lazy val gcp = Seq(
    "com.google.cloud" % "google-cloud-dataproc" % "2.3.2" % "provided",
    "com.google.cloud" % "google-cloud-storage" % "2.4.2" % "provided"
  )
}
