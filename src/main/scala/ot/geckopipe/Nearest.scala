package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.geckopipe.index.V2GIndex.Component
import ot.geckopipe.index.{GeneIndex, VariantIndex}
import ot.geckopipe.index.Indexable._

object Nearest extends LazyLogging {

  // val features: Seq[String] = Seq("d", "inv_d", "biotype")
  val features: Seq[String] = Seq("d", "inv_d")
  val columns: Seq[String] =
    Seq("chr_id", "position", "ref_allele", "alt_allele", "gene_id") ++ features

  def apply(vIdx: VariantIndex, conf: Configuration)(implicit ss: SparkSession): Component = {
    Nearest(vIdx, conf, conf.nearest.tssDistance, Set("protein_coding"))
  }
  def apply(vIdx: VariantIndex, conf: Configuration, tssDistance: Long, biotypes: Set[String])
           (implicit ss: SparkSession): Component = {

    val genes = GeneIndex(conf.ensembl.lut)
      .filterBiotypes(biotypes)
      .sortByTSS
      .table.selectBy(GeneIndex.columns)
      .cache()

    logger.info("generate nearest dataset from variant annotated index")
    val nearests = vIdx.table
      .select(VariantIndex.columns.head, VariantIndex.columns.tail:_*)
      .withColumn("type_id", lit("distance"))
      .withColumn("source_id", lit("canonical_tss"))
      .withColumn("feature", lit("unspecified"))

    val nearestPairs = nearests.join(genes, (col("chr_id") === col("chr")) and
      (abs(col("position") - col("tss")) <= tssDistance))
      .withColumn("d",  abs(col("position") - col("tss")))
      .withColumn("inv_d", lit(1.0) / col("d"))
      .select(columns.head, columns.tail:_*)

    new Component {
      /** unique column name list per component */
      override val features: Seq[String] = Nearest.features
      override val table: DataFrame = nearestPairs
    }
  }
}
