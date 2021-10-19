package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import ot.geckopipe.index._
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderFailures
import scopt.OptionParser
import pureconfig.generic.auto._

class Commands(val ss: SparkSession, val sampleFactor: Double, val c: Configuration)
    extends LazyLogging {
  implicit val sSesion: SparkSession = ss
  implicit val sFactor: Double = sampleFactor

  def variantIndex(): Unit = {
    logger.info("exec variant-index command")
    val vidx = VariantIndex.builder(c).build
    vidx.table.write.parquet(c.variantIndex.path)
  }

  def distanceNearest(): Unit = {
    logger.info("exec distance-nearest command")
    val vIdx = VariantIndex.builder(c).load

    val nearestDF = Distance(vIdx, c)
    nearestDF.table.write.json(c.nearest.path)
  }

  def variantDiseaseColoc(): Unit = {
    logger.info("exec distance-nearest command")
    val variantColumns = VariantIndex.columns.map(col)
    val vIdx = VariantIndex
      .builder(c)
      .load
      .table
      .select(variantColumns: _*)

    val geneColumns = (GeneIndex.indexColumns :+ GeneIndex.idColumn) map col
    val gIdx = GeneIndex(c.ensembl.lut).sortByID.table
      .select(geneColumns: _*)

    val columnsToDrop = VariantIndex.columns ++ GeneIndex.indexColumns :+ GeneIndex.idColumn

    val coloc = ss.read
      .parquet(c.variantDisease.coloc)
      .join(broadcast(gIdx),
            col("left_chrom") === col("chr") and
              col("right_gene_id") === col("gene_id"),
            "left_outer")
      .where(col("right_gene_id").isNull or
        col("right_gene_id") === col("gene_id"))
      .filter(!isnan(col("coloc_h3")))

    val colocVariantFiltered = coloc
      .join(
        vIdx,
        (col("right_chrom") === col("chr_id")) and
          (col("right_pos") === col("position")) and
          (col("right_ref") === col("ref_allele")) and
          (col("right_alt") === col("alt_allele"))
      )
      .drop(columnsToDrop: _*)

    colocVariantFiltered.write
      .format(c.format)
      .save(c.output.stripSuffix("/").concat("/v2d_coloc/"))
  }

  def variantToGene(): Unit = {
    logger.info("exec variant-gene command")

    val vIdx = VariantIndex.builder(c).load
    vIdx.table.cache()

    val vepDts = VEP(c)

    val nearestDts = Distance(vIdx, c)

    val positionalDts = QTL(vIdx, c)

    val intervalDt = Interval(vIdx, c)

    val dtSeq = Seq(vepDts, nearestDts, positionalDts, intervalDt)
    val v2g = V2GIndex.build(dtSeq, c)

    v2g.table.write.format(c.format).save(c.variantGene.path)
  }

  def scoredDatasets(): Unit = {
    logger.info("exec variant-gene-scored command")

    // chr_id, position, ref_allele, alt_allele, gene_id,
    val cols = List("chr_id" -> "tag_chrom",
                    "position" -> "tag_pos",
                    "ref_allele" -> "tag_ref",
                    "alt_allele" -> "tag_alt",
                    "gene_id" -> "gene_id")
    val v2g = V2GIndex.load(c)
    val d2v2g = ss.read.format(c.format).load(c.diseaseVariantGene.path)
    val v2gScores = v2g.computeScores(c).persist()

    val selectC = cols.init.map(p => col(p._1).as(p._2)) :+ col(cols.last._2)
    val d2v2gScores = v2gScores
      .select(selectC: _*)
      .join(
        d2v2g
          .select(cols.map(p => col(p._2)): _*)
          .distinct
          .orderBy(cols.take(2).map(p => col(p._2)): _*),
        cols.map(_._2),
        "left_semi"
      )

    val d2v2gScored = d2v2g
      .join(v2gScores.select(selectC: _*), cols.map(_._2))
      .drop()

    v2gScores.write
      .format(c.format)
      .save(c.scoredDatasets.variantGeneByOverall)

    d2v2gScores.write
      .format(c.format)
      .save(c.scoredDatasets.diseaseVariantGeneByOverall)

    d2v2gScored.write
      .format(c.format)
      .save(c.scoredDatasets.diseaseVariantGeneScored)

  }

  def variantToDisease(): Unit = {
    logger.info("exec variant-disease command")

    val vIdx = VariantIndex.builder(c).load
    val v2d = V2DIndex.build(vIdx, c)

    v2d.table.write.format(c.format).save(c.variantDisease.path)
  }

  def diseaseToVariantToGene(): Unit = {
    logger.info("exec disease-variant-gene command")

    val v2g = V2GIndex.load(c)
    val v2d = V2DIndex.load(c)

    // v2d also contains rows with both null and we dont want those to be included
    val _ = v2d.table
      .where(col("overall_r2").isNotNull or col("posterior_prob").isNotNull)
      .join(
        v2g.table,
        col("chr_id") === col("tag_chrom") and
          (col("position") === col("tag_pos")) and
          (col("ref_allele") === col("tag_ref")) and
          (col("alt_allele") === col("tag_alt"))
      )
      .drop(VariantIndex.columns: _*)
      .write
      .format(c.format)
      .save(c.diseaseVariantGene.path)
  }

  def dictionaries(): Unit = {
    logger.info("exec variant-gene-luts command")

    logger.info("generate lut for variant index")
    VariantIndex
      .builder(c)
      .load
      .flatten
      .table
      .write
      .format(c.format)
      .save(c.output.stripSuffix("/").concat("/lut/variant-index/"))

    logger.info("generate lut for studies")

    V2DIndex
      .buildStudiesIndex(c.variantDisease.studies, c.variantDisease.efos)
      .write
      .format(c.format)
      .save(c.output.stripSuffix("/").concat("/lut/study-index/"))

    logger.info("generate lut for overlapping index")
    V2DIndex
      .buildOverlapIndex(c.variantDisease.overlapping)
      .write
      .format(c.format)
      .save(c.output.stripSuffix("/").concat("/lut/overlap-index/"))

    GeneIndex(c.ensembl.lut).sortByID.table.write
      .format(c.format)
      .save(c.output.stripSuffix("/").concat("/lut/genes-index/"))
  }

  def buildAll(): Unit = {
    variantIndex()
    dictionaries()
    variantDiseaseColoc()
    variantToDisease()
    variantToGene()
    diseaseToVariantToGene()
    scoredDatasets()
  }
}

case class CommandLineArgs(file: String = "",
                           kwargs: Map[String, String] = Map(),
                           command: Option[String] = None)

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

  def run(clArgs: CommandLineArgs, configuration: Configuration)(
      implicit ss: SparkSession): Unit = {
    println(s"running $progName")

    logger.debug(s"running with cli args $clArgs and with configuracion $configuration")

    val cmds = new Commands(ss, configuration.sampleFactor, configuration)

    logger.info("check command specified")
    clArgs.command match {
      case Some("variant-index") =>
        cmds.variantIndex()

      case Some("distance-nearest") =>
        cmds.distanceNearest()

      case Some("variant-disease-coloc") =>
        cmds.variantDiseaseColoc()

      case Some("variant-gene") =>
        cmds.variantToGene()

      case Some("scored-datasets") =>
        cmds.scoredDatasets()

      case Some("variant-disease") =>
        cmds.variantToDisease()

      case Some("disease-variant-gene") =>
        cmds.diseaseToVariantToGene()

      case Some("dictionaries") =>
        cmds.dictionaries()

      case Some("build-all") =>
        cmds.buildAll()

      case _ =>
        logger.error("failed to specify a command to run try --help")
    }

    println("closing app... done.")
  }

  private def getOrCreateSparkSession(sparkUri: Option[String]) = {
    logger.info(s"create spark session with uri:'${sparkUri.toString}'")
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(progName)
      .set("spark.driver.maxResultSize", "0")
      .set("spark.debug.maxToStringFields", "2000")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")

    // if some uri then setmaster must be set otherwise
    // it tries to get from env if any yarn running
    val conf = sparkUri match {
      case Some(uri) if uri.nonEmpty => sparkConf.setMaster(uri)
      case _                         => sparkConf
    }

    SparkSession.builder
      .config(conf)
      .getOrCreate
  }

  private def readConfiguration(configFile: String): Either[ConfigReaderFailures, Configuration] = { //implicit reader used to read the config file

    val conf = if (configFile.nonEmpty) {
      logger.info(s"loading configuration from commandline as $configFile")
      ConfigSource.default(ConfigSource.file(configFile)).load[Configuration]
    } else {
      logger.info("load configuration from package resource")
      ConfigSource.default.load[Configuration]
    }
    conf
  }

  def main(args: Array[String]): Unit = {
    // parser.parse returns Option[C]
    parser.parse(args, CommandLineArgs()) match {
      case Some(config) =>
        readConfiguration(config.file) match {
          case Right(configuration) =>
            implicit val ss: SparkSession = getOrCreateSparkSession(configuration.sparkUri)
            ss.sparkContext.setLogLevel(configuration.logLevel)
            try {
              run(config, configuration)
            } finally {
              ss.close()
            }
          case Left(failures) =>
            println(s"configuration contains errors like ${failures.toString}")
        }

      case None => println("problem parsing commandline args")
    }
  }

  val parser: OptionParser[CommandLineArgs] =
    new OptionParser[CommandLineArgs](progName) {
      head(progName)

      opt[String]("file")
        .abbr("f")
        .valueName("<config-file>")
        .action((x, c) => c.copy(file = x))
        .text("file contains the configuration needed to run the pipeline")

      opt[Map[String, String]]("kwargs")
        .valueName("k1=v1,k2=v2...")
        .action((x, c) => c.copy(kwargs = x))
        .text("other arguments")

      cmd("distance-nearest")
        .action((_, c) => c.copy(command = Some("distance-nearest")))
        .text("generate distance nearest based dataset")

      cmd("variant-disease-coloc")
        .action((_, c) => c.copy(command = Some("variant-disease-coloc")))
        .text("load coloc and filter by gene table and variant index")

      cmd("variant-index")
        .action((_, c) => c.copy(command = Some("variant-index")))
        .text("generate variant index from VEP file")

      cmd("variant-gene")
        .action((_, c) => c.copy(command = Some("variant-gene")))
        .text("generate variant to gene table")

      cmd("scored-datasets")
        .action((_, c) => c.copy(command = Some("scored-datasets")))
        .text("generate scored tables from v2g and d2v2g")

      cmd("variant-disease")
        .action((_, c) => c.copy(command = Some("variant-disease")))
        .text("generate variant to disease table")

      cmd("disease-variant-gene")
        .action((_, c) => c.copy(command = Some("disease-variant-gene")))
        .text("generate disease to variant to gene table")

      cmd("dictionaries")
        .action((_, c) => c.copy(command = Some("dictionaries")))
        .text("generate variant to gene lookup tables")

      cmd("build-all")
        .action((_, c) => c.copy(command = Some("build-all")))
        .text("generate variant index, dictionaries, v2d, v2g, and d2v2g")

      note(entryText)

      override def showUsageOnError = true
    }
}
