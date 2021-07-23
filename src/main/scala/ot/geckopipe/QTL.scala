package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.geckopipe.functions._
import ot.geckopipe.index.V2GIndex.Component
import ot.geckopipe.index.VariantIndex

object QTL extends LazyLogging {
  val features: Seq[String] = Seq("qtl_beta", "qtl_se", "qtl_pval", "qtl_score", "qtl_score_q")

  def load(from: String)(implicit ss: SparkSession): DataFrame = {
    val qtl = ss.read
      .parquet(from)
      .withColumnRenamed("chrom", "chr_id")
      .withColumnRenamed("pos", "position")
      .withColumnRenamed("other_allele", "ref_allele")
      .withColumnRenamed("effect_allele", "alt_allele")
      .withColumnRenamed("beta", "qtl_beta")
      .withColumnRenamed("se", "qtl_se")
      .withColumnRenamed("pval", "qtl_pval")
      .withColumnRenamed("ensembl_id", "gene_id")
      .withColumnRenamed("type", "type_id")
      .withColumnRenamed("source", "source_id")

    qtl
  }

  /** union all intervals and interpolate variants from intervals */
  def apply(vIdx: VariantIndex, conf: Configuration)(implicit ss: SparkSession): Component = {
    import ss.implicits._

    logger.info("generate pchic dataset from file and aggregating by range and gene")
    val qtls = load(conf.qtl.path)
    // TODO WARN this is a temporal hack until we fix the qtl dataset and properly capture 2230 smallest entries
      .withColumn("qtl_pval",
                  when($"qtl_pval" === 0d, lit(Double.MinPositiveValue)).otherwise($"qtl_pval"))
      .withColumn("qtl_score", -log(10, col("qtl_pval")))

    val vIdxS = vIdx.table.select(VariantIndex.columns.head, VariantIndex.columns.tail: _*)
    val qtlTable = qtls.join(vIdxS, VariantIndex.columns, "left_semi")

    val qtlWP = qtlTable
      .transform(computePercentiles(_, "qtl_score", "qtl_score_q"))

    new Component {

      /** unique column name list per component */
      override val features: Seq[String] = QTL.features
      override val table: DataFrame = qtlWP
    }
  }
}
