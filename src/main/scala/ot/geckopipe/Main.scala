package ot.geckopipe

import ch.qos.logback.classic.Logger
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scopt.OptionParser

case class CommandLineArgs(gtex: String = "", out: String = "",
                           uri: String = "local[*]",
                           sample: Double = 0,
                           kwargs: Map[String,String] = Map())

object Main extends LazyLogging {
  val progVersion = "0.1"
  val progName = "gecko-pipe"

  def run(config: CommandLineArgs): SparkSession = {
    val logLevel = config.kwargs.getOrElse("log-level", "WARN")
    logger.info(s"runnning with cli args $config")
    val conf: SparkConf = new SparkConf()
      .setAppName(progName)
      .setMaster(s"${config.uri}")

    implicit val ss: SparkSession = SparkSession.builder
      .config(conf)
      .getOrCreate

    // needed to use the $notation
    import ss.implicits._

    logger.info("setting sparkcontext logging level to WARN or log-level from cli args")
    // set log level to WARN
    ss.sparkContext.setLogLevel(logLevel)

    val gtexDF = GTEx.load(config.gtex, config.sample)

    val numLines = gtexDF.count()
    logger.info(s"number of lines in GTEx dataset $numLines using sample fraction ${config.sample}")
    ss
  }

  def main(args: Array[String]) {
    // parser.parse returns Option[C]
    parser.parse(args, CommandLineArgs()) match {
      case Some(config) =>
        run(config).stop
      case None =>
    }
  }

  val parser:OptionParser[CommandLineArgs] = new OptionParser[CommandLineArgs](progName) {
    head(progName, progVersion)

    opt[String]('s', "spark-uri")
      .valueName("<spark-session|local[*]>")
      .action( (x, c) => c.copy(uri = x) )
      .text("spark session uri and by default 'local[*]'")

    opt[String]('g', "gtex").required()
      .valueName("<file>")
      .action( (x, c) => c.copy(gtex = x) )
      .text("gtex filename path")

    opt[String]('o', "out").required()
      .valueName("<folder>")
      .action( (x, c) => c.copy(out = x) )
      .text("out folder to save computed rdd partitions")

    opt[Double]('a', "sample").required()
      .valueName("fraction")
      .action( (x, c) => c.copy(sample = x) )
      .text("sample number to get from the data: .0 by default")

    opt[Map[String,String]]("kwargs")
      .valueName("k1=v1,k2=v2...")
      .action( (x, c) => c.copy(kwargs = x) )
      .text("other arguments")

    note("You must to specify in and out parameters\n")
  }
}
