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
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scopt.OptionParser

import scala.util.{Failure, Success}

case class CommandLineArgs(file: String = "", kwargs: Map[String,String] = Map())

object Main extends LazyLogging {
  val progVersion = "0.2"
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
    println(s"running ${progName} version ${progVersion}")
    val conf = if (config.file.nonEmpty) {
        logger.info(s"loading configuration from commandline as ${config.file}")
        pureconfig.loadConfig[Configuration](Paths.get(config.file))
      } else {
        logger.info("load configuration from package resource")
        pureconfig.loadConfig[Configuration]
      }

    conf match {
      case Right(c) =>
        logger.debug(s"running with cli args $config and with default configuracion ${c}")
        val logLevel = c.logLevel

        val conf: SparkConf =
          if (c.sparkUri.nonEmpty)
            new SparkConf()
              .setAppName(progName)
              .setMaster(s"${c.sparkUri}")
          else
            new SparkConf()
              .setAppName(progName)


        implicit val ss: SparkSession = SparkSession.builder
          .config(conf)
          .getOrCreate

        logger.debug("setting sparkcontext logging level to log-level")
        ss.sparkContext.setLogLevel(logLevel)

        val gtex = Dataset.buildGTEx(c)
        gtex.show

        val vep = Dataset.buildVEP(c)
        vep.show

        val gtexAndVep = gtex.join(vep, Seq("variant_id"), "inner")
        gtexAndVep.show

        // persist the created table
        // gtexEGenesDF.createOrReplaceTempView("gtex_egenes")
        // ss.table("gtex").persist(StorageLevel.MEMORY_AND_DISK)

//        val qvalCount = ss.sql(s"""
//          SELECT count(*)
//          FROM gtex
//          WHERE (pval_nominal <= 0.05)
//          """).show()

        ss.stop

      case Left(failures) => println(s"configuration contains errors like ${failures.toString}")
    }
    println("closing app... done.")
  }

  def main(args: Array[String]) {
    // parser.parse returns Option[C]
    parser.parse(args, CommandLineArgs()) match {
      case Some(config) =>
        run(config)
      case None => println("problem parsing commandline args")
    }
  }

  val parser:OptionParser[CommandLineArgs] = new OptionParser[CommandLineArgs](progName) {
    head(progName, progVersion)

    opt[String]('f', "file")
      .valueName("<config-file>")
      .action( (x, c) => c.copy(file = x) )
      .text("file contains the configuration needed to run the pipeline")

    opt[Map[String,String]]("kwargs")
      .valueName("k1=v1,k2=v2...")
      .action( (x, c) => c.copy(kwargs = x) )
      .text("other arguments")

    note(entryText)
  }
}
