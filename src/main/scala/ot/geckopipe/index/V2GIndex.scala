package ot.geckopipe.index

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import ot.geckopipe.functions._
import ot.geckopipe.{Chromosomes, Configuration}

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
    val inChrCount = table.where($"chr_id".isin(Chromosomes.chrList:_*)).count()

    logger.info(s"count number of rows in chr range $inChrCount of a total $totalRows")
    Seq(inChrCount, totalRows)
  }
}

object V2GIndex extends LazyLogging  {
  trait Component {
    /** unique column name list per component */
    val features: Seq[String]
    val table: DataFrame
  }

  val schema =
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
      StructField("max_qtl", DoubleType) ::
      StructField("max_int", DoubleType) ::
      StructField("max_fpred", DoubleType) ::
      StructField("d", DoubleType) ::
      StructField("distance_score", DoubleType) ::
      StructField("distance_score_q", DoubleType) ::
      StructField("source_score", DoubleType) ::
      StructField("overall_score", DoubleType) :: Nil)

  /** all data sources to incorporate needs to meet this format at the end
    *
    * One example of the shape of the data could be
    * "1_123_T_C ENSG0000001 gtex uberon_0001 1
    */
  val features: Seq[String] = Seq("feature", "type_id", "source_id")

  /** columns to index the dataset */
  val indexColumns: Seq[String] = Seq("chr_id", "position")
  /** the whole list of columns this dataset will be outputing */
  val columns: Seq[String] = (VariantIndex.columns ++ GeneIndex.idColumns ++ features).distinct

  /** join built gtex and vep together and generate char pos alleles columns from variant_id */
  def build(datasets: Seq[Component], vIdx: VariantIndex, conf: Configuration)
           (implicit ss: SparkSession): V2GIndex = {

    logger.info("build variant to gene dataset union the list of datasets")

    val allFeatures =
      datasets.foldLeft(Seq[String]())((agg, el) => agg ++ el.features).distinct

    val processedDts = datasets.map( el =>
      (allFeatures diff el.features).foldLeft(el.table)((agg, el) => agg.withColumn(el, lit(null)))
    )

    val allDts = concatDatasets(processedDts, (columns ++ allFeatures).distinct)
    new V2GIndex(allDts)
  }

  /** join built gtex and vep together and generate char pos alleles columns from variant_id */
  def load(conf: Configuration)(implicit ss: SparkSession): V2GIndex = {

    logger.info("load variant to gene dataset from built one")
    val v2g = ss.read
      .schema(schema)
      .json(conf.variantGene.path)

    new V2GIndex(v2g)
  }
}
