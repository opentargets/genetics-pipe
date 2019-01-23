package ot.geckopipe

import java.nio.file.Paths

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import ot.geckopipe.index._
import ot.geckopipe.functions._
import scopt.OptionParser
import org.apache.spark.sql.functions._
import pureconfig.generic.auto._

class Commands(val ss: SparkSession, val sampleFactor: Double, val c: Configuration) extends LazyLogging {
  implicit val sSesion: SparkSession = ss
  implicit val sFactor: Double = sampleFactor

  def variantIndex(): Unit = {
    logger.info("exec variant-index command")
    val _ = VariantIndex.builder(c).build
  }

  def variantToGene(): Unit = {
    logger.info("exec variant-gene command")

    val vIdx = VariantIndex.builder(c).load

    val vepDts = VEP(c)
    val positionalDts = QTL(vIdx, c)
    val intervalDt = Interval(vIdx, c)

    val dtSeq = Seq(vepDts, positionalDts, intervalDt)
    val v2g = V2GIndex.build(dtSeq, vIdx, c)

    v2g.saveToJSON(c.output.stripSuffix("/").concat("/v2g/"))
  }

  def variantToDisease(): Unit = {
    logger.info("exec variant-disease command")

    val vIdx = VariantIndex.builder(c).load
    val v2d = V2DIndex.build(vIdx, c)

    v2d.saveToJSON(c.output.stripSuffix("/").concat("/v2d/"))
  }

  def diseaseToVariantToGene(): Unit = {
    logger.info("exec variant-disease command")

    val v2g = V2GIndex.load(c)
    val v2d = V2DIndex.load(c)

    // v2d also contains rows with both null and we dont want those to be included
    val merged = v2d.table
      .where(col("r2").isNotNull or col("posterior_prob").isNotNull)
      .join(v2g.table, VariantIndex.columns)

    saveToJSON(merged, c.output.stripSuffix("/").concat("/d2v2g/"))
  }

  def dictionaries(): Unit = {
    logger.info("exec variant-gene-luts command")

    val vIdxBuilder = VariantIndex.builder(c)
    val vIdx = vIdxBuilder.load
    val nearests = vIdxBuilder.loadNearestGenes.map( df => {
      logger.info("generate variant index LUT with nearest genes (prot-cod and not prot-cod")
      vIdx.table.join(df, VariantIndex.variantColumnNames, "left_outer")
    })

    nearests match {
      case scala.util.Success(table) =>
        logger.info("write to json variant index LUT")
        table.write.json(c.output.stripSuffix("/").concat("/variant-index-lut/"))
      case scala.util.Failure(ex) => logger.error(ex.getMessage)
    }
  }
}

case class CommandLineArgs(file: String = "", kwargs: Map[String,String] = Map(), command: Option[String] = None)

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

        val cmds = new Commands(ss, sampleFactor, c)

        logger.info("check command specified")
        config.command match {
          case Some("variant-index") =>
            cmds.variantIndex()

          case Some("variant-gene") =>
            cmds.variantToGene()

          case Some("variant-disease") =>
            cmds.variantToDisease()

          case Some("disease-variant-gene") =>
            cmds.diseaseToVariantToGene()

          case Some("dictionaries") =>
            cmds.dictionaries()

          case _ =>
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
      .action( (_, c) => c.copy(command = Some("variant-index")) )
      .text("generate variant index from VEP file")

    cmd("variant-gene").
      action( (_, c) => c.copy(command = Some("variant-gene")))
      .text("generate variant to gene table")

    cmd("variant-disease").
      action( (_, c) => c.copy(command = Some("variant-disease")))
      .text("generate variant to disease table")

    cmd("disease-variant-gene").
      action( (_, c) => c.copy(command = Some("disease-variant-gene")))
      .text("generate disease to variant to gene table")

    cmd("dictionaries").
      action( (_, c) => c.copy(command = Some("dictionaries")))
      .text("generate variant to gene lookup tables")

    note(entryText)

    override def showUsageOnError = true
  }
}
