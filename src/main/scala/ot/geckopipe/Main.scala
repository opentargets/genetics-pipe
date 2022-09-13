package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import ot.geckopipe.domain.Manhattan
import ot.geckopipe.index._
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderFailures
import pureconfig.generic.auto._

class Commands(val c: Configuration)(implicit val ss: SparkSession) extends LazyLogging {

  def variantIndex(): Unit = {
    logger.info("exec variant-index command")
    val vidx = VariantIndex.builder(c).build
    vidx.table.write.parquet(c.variantIndex.path)
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
      .join(
        broadcast(gIdx),
        col("left_chrom") === col("chr") and
          col("right_gene_id") === col("gene_id"),
        "left_outer"
      )
      .where(
        col("right_gene_id").isNull or
          col("right_gene_id") === col("gene_id")
      )
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
    val cols = List(
      "chr_id" -> "tag_chrom",
      "position" -> "tag_pos",
      "ref_allele" -> "tag_ref",
      "alt_allele" -> "tag_alt",
      "gene_id" -> "gene_id"
    )
    val v2g = V2GIndex.load(c)
    val d2v2g = ss.read.format(c.format).load(c.diseaseVariantGene.path)
    val v2gScores = v2g.computeScores(c).orderBy(cols.take(2).map(x => col(x._1)): _*).persist()
    val v2gScoresRenamed = cols.foldLeft(v2gScores)((B, a) => B.withColumnRenamed(a._1, a._2))

    val d2v2gScored = d2v2g
      .join(v2gScoresRenamed, cols.map(_._2))

    val v2gScored = v2g.table.join(v2gScores, cols.map(_._1))
    v2gScored.write
      .format(c.format)
      .save(c.scoredDatasets.variantGeneScored)

    d2v2gScored.write
      .format(c.format)
      .option("maxRecordsPerFile", 1000000)
      .save(c.scoredDatasets.diseaseVariantGeneScored)

  }

  def manhattan(): Unit = {
    Manhattan(c).write.format(c.format).save(c.manhattan.path)
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

  def search(): Unit = {
    val searchFormat = "json"
    val searchPath = c.output.stripSuffix("/").concat("/search")

    logger.info("Generate search table: Variant")
    VariantIndex
      .builder(c)
      .load
      .flatten
      .table
      .select(
        concat_ws(
          "_",
          col("chr_id"),
          col("position"),
          col("ref_allele"),
          col("alt_allele")
        ) as "variant_id",
        col("rs_id")
      )
      .write
      .format(searchFormat)
      .save(searchPath.concat("/variant"))

    logger.info("Generate search table: Studies")
    V2DIndex
      .buildStudiesIndex(c.variantDisease.studies, c.variantDisease.efos)
      .select(
        "study_id",
        "pmid",
        "num_assoc_loci",
        "pub_author",
        "pub_journal",
        "pub_title",
        "trait_reported"
      )
      .write
      .format(searchFormat)
      .save(searchPath.concat("/study"))

    // gene_id, gene_name
    logger.info("Generate search table: Genes")
    GeneIndex(c.ensembl.lut).table
      .select("gene_id", "gene_name")
      .write
      .format(searchFormat)
      .save(searchPath.concat("/gene"))
  }
  def buildAll(): Unit = {
    variantIndex()
    dictionaries()
    search()
    variantDiseaseColoc()
    variantToDisease()
    variantToGene()
    diseaseToVariantToGene()
    scoredDatasets()
    manhattan()
  }
}

object Main extends LazyLogging {

  private def getOrCreateSparkSession(conf: Configuration) = {
    logger.info(s"create spark session with uri:'${conf.sparkUri.toString}'")
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(conf.programName)
      .set("spark.driver.maxResultSize", "0")
      .set("spark.debug.maxToStringFields", "2000")
      // fixme: why is this disabled? Why not use broadcast joins?
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")

    SparkSession.builder
      .config(conf.sparkUri match {
        case Some(sparkUri) => sparkConf.setMaster(sparkUri)
        case None           => sparkConf
      })
      .getOrCreate
  }

  case class Session(sparkSession: SparkSession, configuration: Configuration)

  private def getSessionContext: Option[Session] = {
    val context: Either[ConfigReaderFailures, Session] = for {
      config <- Configuration.config
    } yield {
      val spark = getOrCreateSparkSession(config)
      Session(spark, config)
    }
    context match {
      case Left(invalid) =>
        logger.error(s"Unable to generate session context. Configuration errors:")
        invalid.toList foreach { config_error =>
          logger.warn(config_error.description)

        }
        None
      case Right(ctx) => Some(ctx)
    }
  }

  def main(args: Array[String]): Unit = {
    getSessionContext match {
      case Some(ctx) => args.foreach(runEtlStep(_, ctx))
      case None =>
        logger.info("Unable to generate session context.")
        sys.exit(0)
    }

  }

  def runEtlStep(step: String, context: Session): Unit = {
    val cmds: Commands = new Commands(context.configuration)(context.sparkSession)
    step match {
      case "distance-nearest"      => cmds.variantIndex()
      case "variant-disease-coloc" => cmds.variantDiseaseColoc()
      case "variant-index"         => cmds.variantIndex()
      case "variant-gene"          => cmds.variantToGene()
      case "scored-datasets"       => cmds.scoredDatasets()
      case "variant-disease"       => cmds.variantToDisease()
      case "disease-variant-gene"  => cmds.diseaseToVariantToGene()
      case "dictionaries"          => cmds.dictionaries()
      case "manhattan"             => cmds.manhattan()
      case "search"                => cmds.search()
      case _ =>
        logger.warn(s"Unrecognised step $step. Exiting...")
        sys.exit(0)
    }
  }

}
