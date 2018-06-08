package ot.geckopipe

import java.nio.file.Paths

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import ot.geckopipe.index.VariantIndex
import ot.geckopipe.interval.{Fantom5, PCHIC}
import ot.geckopipe.positional.{GTEx, VEP}
import scopt.OptionParser

case class CommandLineArgs(file: String = "", kwargs: Map[String,String] = Map())

object Main extends LazyLogging {
  val progVersion: String = "0.11"
  val progName: String = "gecko-pipe"
  val entryText: String =
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
    println(s"running $progName version $progVersion")
    val conf = if (config.file.nonEmpty) {
        logger.info(s"loading configuration from commandline as ${config.file}")
        pureconfig.loadConfig[Configuration](Paths.get(config.file))
      } else {
        logger.info("load configuration from package resource")
        pureconfig.loadConfig[Configuration]
      }

    conf match {
      case Right(c) =>
        logger.debug(s"running with cli args $config and with default configuracion $c")
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

        // needed for save dataset function
        implicit val sampleFactor: Double = c.sampleFactor

        val vIdx = VariantIndex.builder(c).loadOrBuild

        // vIdx.table.show(50, false)
        val numV = vIdx.table.count
        val numVwRS = vIdx.table.where(col("rs_id").isNotNull).count

        logger.info(s"number of variants $numV and with rsID $numVwRS")

        val gtex = GTEx(c)
        val vep = VEP(c)
        val positionalSeq = Seq(gtex, vep)

        val pchic = PCHIC(c)
        val dhs = Fantom5(c)
        val intervalSeq = Seq(pchic, dhs)

        val intervalDts = Variant2Gene.buildIntervals(vIdx, intervalSeq, c)

        val dtSeq = intervalDts.foldLeft(positionalSeq)( (agg, ds) => agg :+ ds)
        val v2g = Variant2Gene(dtSeq, vIdx, c)

        v2g match {
          case Some(r) =>
            Variant2Gene.saveToFile(r, c.output.stripSuffix("/").concat("/merged/"))
            // r.show(500, truncate = false)
          //
          //        val stats = Dataset.computeStats(gtexAndVep, "dataset")
          //
          //        println(s"few numbers from stats in chr count ${stats(0)} and total rows ${stats(1)}")
          case None => logger.error("failed to generate any build variant to gene dataset." +
            "This should not be happening")
        }

        ss.stop

      case Left(failures) => println(s"configuration contains errors like ${failures.toString}")
    }
    println("closing app... done.")
  }

  def main(args: Array[String]): Unit = {
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
