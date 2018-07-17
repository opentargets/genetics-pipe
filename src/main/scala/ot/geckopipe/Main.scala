package ot.geckopipe

import java.nio.file.Paths

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import ot.geckopipe.index.{EnsemblIndex, V2DIndex, V2GIndex, VariantIndex}
import ot.geckopipe.interval.Interval
import ot.geckopipe.qtl.QTL
import ot.geckopipe.functions._
import scopt.OptionParser
import org.apache.spark.sql.functions._

sealed trait Command
case class VICmd() extends Command
case class V2GCmd() extends Command
case class V2DCmd() extends Command
case class D2V2GCmd() extends Command
case class V2GLUTCmd() extends Command
case class V2GStatsCmd() extends Command

case class CommandLineArgs(file: String = "", kwargs: Map[String,String] = Map(), command: Option[Command] = None)

object Main extends LazyLogging {
  val progName: String = "ot-geckopipe"
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
    println(s"running $progName")
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

        logger.info("check command specified")
        config.command match {
          case Some(_: VICmd) =>
            logger.info("exec variant-index command")

            val _ = VariantIndex.builder(c).build

          case Some(_: V2GCmd) =>
            logger.info("exec variant-gene command")

            val vIdx = VariantIndex.builder(c).load

            val vepDts = VEP(c)
            val positionalDts = QTL(vIdx, c)
            val intervalDt = Interval(vIdx, c)

            val dtSeq = Seq(vepDts, positionalDts, intervalDt)
            val v2g = V2GIndex.build(dtSeq, vIdx, c)

            v2g.save(c.output.stripSuffix("/").concat("/v2g/"))

          case Some(_: V2DCmd) =>
            logger.info("exec variant-disease command")

            val vIdx = VariantIndex.builder(c).load
            val v2d = V2DIndex.build(vIdx, c)

            v2d.save(c.output.stripSuffix("/").concat("/v2d/"))

          case Some(_: D2V2GCmd) =>
            logger.info("exec variant-disease command")

            val v2g = V2GIndex.load(c)
            val v2d = V2DIndex.load(c)

            val merged = v2d.table.join(v2g.table, VariantIndex.columns)

            saveToCSV(merged, c.output.stripSuffix("/").concat("/d2v2g/"))

          case Some(_: V2GLUTCmd) =>
            logger.info("exec variant-gene-luts command")

            val vIdx = VariantIndex.builder(c).load

            logger.info("write rs_id to chr-position")
            vIdx.selectBy(Seq("rs_id", "chr_id", "position"))
              .orderBy(col("rs_id").asc)
              .distinct()
              .write
              .option("delimiter","\t")
              .option("header", "false")
              .csv(c.output.stripSuffix("/").concat("/v2g-lut-rsid/"))

            logger.info("write gene name to chr position")
            val _ = EnsemblIndex(c.ensembl.geneTranscriptPairs)
              .aggByGene
              .select("gene_id", "gene_chr", "gene_start", "gene_end")
              .orderBy(col("gene_id").asc)
              .write
              .option("delimiter","\t")
              .option("header", "false")
              .csv(c.output.stripSuffix("/").concat("/v2g-lut-gene/"))


          case Some(_: V2GStatsCmd) =>
            logger.info("exec variant-gene-stats command")

            val vIdx = VariantIndex.builder(c).load

            val vepDts = VEP(c)
            val positionalDts = QTL(vIdx, c)
            val intervalDt = Interval(vIdx, c)

            val dtSeq = Seq(vepDts, positionalDts, intervalDt)
            val v2g = V2GIndex.load(c)

            val stats = v2g.computeStats
            logger.info(s"computed stats $stats")

          case None =>
            logger.error("failed to specify a command to run try --help")
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
    head(progName)

    opt[String]("file")
      .abbr("f")
      .valueName("<config-file>")
      .action( (x, c) => c.copy(file = x) )
      .text("file contains the configuration needed to run the pipeline")

    opt[Map[String,String]]("kwargs")
      .valueName("k1=v1,k2=v2...")
      .action( (x, c) => c.copy(kwargs = x) )
      .text("other arguments")

    cmd("variant-index")
      .action( (_, c) => c.copy(command = Some(VICmd())) )
      .text("generate variant index from VEP file")

    cmd("variant-gene").
      action( (_, c) => c.copy(command = Some(V2GCmd())))
      .text("generate variant to gene table")

    cmd("variant-disease").
      action( (_, c) => c.copy(command = Some(V2DCmd())))
      .text("generate variant to disease table")

    cmd("disease-variant-gene").
      action( (_, c) => c.copy(command = Some(D2V2GCmd())))
      .text("generate disease to variant to gene table")

    cmd("variant-gene-luts").
      action( (_, c) => c.copy(command = Some(V2GLUTCmd())))
      .text("generate variant to gene lookup tables for variants, rsids and genes")

    cmd("variant-gene-stats").
      action( (_, c) => c.copy(command = Some(V2GStatsCmd())))
      .text("generate variant to gene stat results")

    note(entryText)

    override def showUsageOnError = true
  }
}
