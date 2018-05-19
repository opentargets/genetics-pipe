package ot.geckopipe

import java.nio.file.{Path, Paths}

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
import ot.geckopipe.GTEx.Tissue
import scopt.OptionParser

import scala.util.{Failure, Success}

case class CommandLineArgs(file: Path, kwargs: Map[String,String] = Map())

object Main extends LazyLogging {
  val defaultConfigFileName = Paths.get("application.conf")
  val progVersion = "0.1"
  val progName = "gecko-pipe"
  val entryText =
    """
      |
      |NOTE:
      |copy logback.xml locally, modify it with desired logger levels and specify
      |-Dlogback.configurationFile=/path/to/customised/logback.xml. Keep in mind
      |that "Logback-classic can scan for changes in its configuration file and
      |automatically reconfigure itself when the configuration file changes".
      |So, you even don't need to relaunch your process to change logging levels
      | -- https://goo.gl/HMXCqY
      |
    """.stripMargin

  def run(config: CommandLineArgs): Unit = {
    logger.debug(s"running ${progName} version ${progVersion}")
    val conf = pureconfig.loadConfig[Configuration](config.file)

    conf match {
      case Right(c) =>
        logger.debug(s"running with cli args $config and with default configuracion ${c}")
        val logLevel = c.logLevel

        val conf: SparkConf = new SparkConf()
          .setAppName(progName)
          .setMaster(s"${c.sparkUri}")

        implicit val ss: SparkSession = SparkSession.builder
          .config(conf)
          .getOrCreate

        // needed to use the $notation
        import ss.implicits._

        logger.info("setting sparkcontext logging level to WARN or log-level from configuration.conf ")
        // set log level to WARN
        ss.sparkContext.setLogLevel(logLevel)

        val tLUT = GTEx.buildTissueLUT(c.gtex.tissueMap)

        logger.whenInfoEnabled {
          logger.info(s"check for a tissue brain cortex if loaded should be " +
            s"${tLUT.getOrElse("Brain_Cortex.v7.egenes.txt.gz",Tissue("empty","")).code}")
        }

        val gtexEGenesDF = GTEx.loadEGenes(c.gtex.egenes, c.gtex.sampleFactor)
        gtexEGenesDF.show(10)
        gtexEGenesDF.createOrReplaceTempView("gtex_egenes")

        val gtexVGPairsDF = GTEx.loadVGPairs(c.gtex.variantGenePairs, c.gtex.sampleFactor)
        gtexVGPairsDF.show(10)
        gtexVGPairsDF.createOrReplaceTempView("gtex_vgpairs")

        // persist the created table
        // ss.table("gtex").persist(StorageLevel.MEMORY_AND_DISK)

//        val qvalCount = ss.sql(s"""
//          SELECT count(*)
//          FROM gtex
//          WHERE (pval_nominal <= 0.05)
//          """).show()

        // val numLines = gtexDF.count()
        // logger.info(s"number of lines in GTEx dataset $numLines using sample fraction ${config.sample}")
        ss.stop

      case Left(failures) => logger.error(s"${failures.toString}")
    }
  }

  def main(args: Array[String]) {
    // parser.parse returns Option[C]
    parser.parse(args, CommandLineArgs(file = defaultConfigFileName)) match {
      case Some(config) =>
        run(config)
      case None =>
    }
  }

  val parser:OptionParser[CommandLineArgs] = new OptionParser[CommandLineArgs](progName) {
    head(progName, progVersion)

    opt[String]('f', "file")
      .valueName("<config-file>")
      .action( (x, c) => c.copy(file = Paths.get(x)) )
      .text("file contains the configuration needed to run the pipeline")

    opt[Map[String,String]]("kwargs")
      .valueName("k1=v1,k2=v2...")
      .action( (x, c) => c.copy(kwargs = x) )
      .text("other arguments")

    note(entryText)
  }
}
