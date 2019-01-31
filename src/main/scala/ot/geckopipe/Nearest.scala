package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.geckopipe.functions._
import ot.geckopipe.index.V2GIndex.Component
import ot.geckopipe.index.{EnsemblIndex, VariantIndex}

object Nearest extends LazyLogging {

  val features: Seq[String] = Seq("tss", "d")
  val columns: Seq[String] =
    Seq("chr_id", "position", "ref_allele", "alt_allele", "gene_id") ++ features

  /** union all intervals and interpolate variants from intervals */
  def apply(vIdx: VariantIndex, conf: Configuration)(implicit ss: SparkSession): Component = {
    val tssDistance = conf.nearest.tssDistance

    val genes = ss.read.json(conf.ensembl.lut)
      .select("gene_id", "biotype", "chr", "tss")
      .repartitionByRange(col("chr").asc)
      .sortWithinPartitions(col("chr").asc, col("tss").asc)
      .cache()

    logger.info("generate nearest dataset from variant annotated index")
    val nearests = vIdx.table
      .withColumn("type_id", lit("distance"))
      .withColumn("source_id", lit("nearest"))
      .withColumn("feature", lit("unspecified"))
      .withColumn("qtl_score", - log(10, col("qtl_pval")))

    val nearestPairs = nearests.join(genes, (col("chr_id") === col("chr")) and
      (abs(col("position") - col("tss")) <= tssDistance))
    new Component {
      /** unique column name list per component */
      override val features: Seq[String] = Nearest.features
      override val table: DataFrame = nearestPairs
    }
  }
}