package ot.geckopipe.index

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import ot.geckopipe.functions._
import ot.geckopipe.{Chromosomes, Configuration}
import ot.geckopipe.index.Indexable._

/** represents a cached table of variants with all variant columns
  *
  * columns as chr_id, position, ref_allele, alt_allele, variant_id, rs_id. Also
  * this table is persisted and sorted by (chr_id, position) by default
  */
class V2GIndex(val table: DataFrame) extends LazyLogging {

  /** compute stats with this resulted table but only when info enabled */
  def computeStats(implicit ss: SparkSession): Seq[Long] = {
    import ss.implicits._
    val totalRows = table.count()
    // val rsidNullsCount = dataset.where($"rsid".isNull).count()
    val inChrCount = table.where($"chr_id".isin(Chromosomes.chrList: _*)).count()

    logger.info(s"count number of rows in chr range $inChrCount of a total $totalRows")
    Seq(inChrCount, totalRows)
  }

  /** compute the scores per datasource and per overall */
  def computeScores(configuration: Configuration)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    val cols = Seq(
      "source_id",
      "chr_id",
      "position",
      "ref_allele",
      "alt_allele",
      "gene_id"
    )

    val weights: DataFrame = ss.read.json(configuration.variantGene.weights)
    val weightSum: Double = weights.select("weight").collect().map(_.getDouble(0)).sum

    table
      .groupBy(cols.map(col): _*)
      .agg(
        max(coalesce($"qtl_score_q", lit(0d))).as("max_qtl"),
        max(coalesce($"interval_score_q", lit(0d))).as("max_int"),
        max(coalesce($"fpred_max_score", lit(0d))).as("max_fpred"),
        max(coalesce($"distance_score_q", lit(0d))).as("max_distance")
      )
      .withColumn("source_score", $"max_qtl" + $"max_int" + $"max_fpred" + $"max_distance")
      .join(broadcast(weights.orderBy($"source_id")), Seq("source_id"))
      .withColumn("source_score_weighted", $"weight" * $"source_score")
      .groupBy(cols.drop(1).map(col): _*)
      .agg(
        collect_list(struct($"source_id", $"source_score")).as("ss_list"),
        (sum($"source_score_weighted") / sum(lit(weightSum))).as("overall_score")
      )
      .withColumn("source_list", $"ss_list.source_id")
      .withColumn("source_score_list", $"ss_list.source_score")
      .drop("ss_list")
  }
}

object V2GIndex extends LazyLogging {

  trait Component {

    /** unique column name list per component */
    val features: Seq[String]
    val table: DataFrame
  }

  val schema: StructType =
    StructType(
      StructField("chr_id", StringType) ::
        StructField("position", LongType) ::
        StructField("ref_allele", StringType) ::
        StructField("alt_allele", StringType) ::
        StructField("gene_id", StringType) ::
        StructField("feature", StringType) ::
        StructField("type_id", StringType) ::
        StructField("source_id", StringType) ::
        StructField("fpred_labels", ArrayType(StringType)) ::
        StructField("fpred_scores", ArrayType(DoubleType)) ::
        StructField("fpred_max_label", StringType) ::
        StructField("fpred_max_score", DoubleType) ::
        StructField("qtl_beta", DoubleType) ::
        StructField("qtl_se", DoubleType) ::
        StructField("qtl_pval", DoubleType) ::
        StructField("qtl_score", DoubleType) ::
        StructField("interval_score", DoubleType) ::
        StructField("qtl_score_q", DoubleType) ::
        StructField("interval_score_q", DoubleType) ::
        StructField("d", LongType) ::
        StructField("distance_score", DoubleType) ::
        StructField("distance_score_q", DoubleType) :: Nil)

  /** all data sources to incorporate needs to meet this format at the end
    *
    * One example of the shape of the data could be
    * "1_123_T_C ENSG0000001 gtex uberon_0001 1
    */
  val features: Seq[String] = Seq("feature", "type_id", "source_id")

  /** columns to index the dataset */
  val indexColumns: Seq[String] = Seq("chr_id", "position")

  /** the whole list of columns this dataset will be outputing */
  val columns: Seq[String] = (VariantIndex.columns ++ features :+ GeneIndex.idColumn).distinct

  /** join built gtex and vep together and generate char pos alleles columns from variant_id */
  def build(datasets: Seq[Component], conf: Configuration)(implicit ss: SparkSession): V2GIndex = {

    logger.info("generate a set of genes resulting from bio exclusion and chromosome lists")
    val geneIDs = broadcast(
      GeneIndex(conf.ensembl.lut).table
        .selectBy(GeneIndex.idColumn :: Nil)
        .orderBy(GeneIndex.idColumn))

    logger.info("build variant to gene dataset union the list of datasets")

    val allFeatures =
      datasets.foldLeft(Seq[String]())((agg, el) => agg ++ el.features).distinct

    logger.info(s"compose the list of features to include: ${allFeatures.mkString("[", ", ", "]")}")
    logger.info(s"and filter by columns: ${schema.fieldNames.mkString("[", ", ", "]")}")
    val processedDts = datasets.map(
      el =>
        (allFeatures diff el.features)
          .foldLeft(el.table)((agg, el) => agg.withColumn(el, lit(null))))

    val allDts = concatDatasets(processedDts, (columns ++ allFeatures).distinct)
      .withColumn("fpred_labels", coalesce(col("fpred_labels"), typedLit(Array.empty[String])))
      .withColumn("fpred_scores", coalesce(col("fpred_scores"), typedLit(Array.empty[Double])))
      .join(geneIDs, Seq(GeneIndex.idColumn), "left_semi")
      .select(schema.fieldNames.head, schema.fieldNames.tail: _*)

    new V2GIndex(allDts)
  }

  /** join built gtex and vep together and generate char pos alleles columns from variant_id */
  def load(conf: Configuration)(implicit ss: SparkSession): V2GIndex = {
    logger.info("load variant to gene dataset from built one")
    val v2g = ss.read
      .schema(schema)
      .format(conf.format)
      .load(conf.variantGene.path)

    new V2GIndex(v2g)
  }
}
