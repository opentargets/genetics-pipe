package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.geckopipe.index.V2GIndex.Component
import ot.geckopipe.functions._
import ot.geckopipe.index.{GeneIndex, VariantIndex}
import ot.geckopipe.index.Indexable._

object Distance extends LazyLogging {

  // val features: Seq[String] = Seq("d", "inv_d", "biotype")
  val features: Seq[String] = Seq("d", "distance_score", "distance_score_q")

  def apply(vIdx: VariantIndex, conf: Configuration)(implicit ss: SparkSession): Component = {
    Distance(vIdx, conf, conf.nearest.tssDistance, GeneIndex.BioTypes.ApprovedBioTypes)
  }

  def apply(vIdx: VariantIndex,
            conf: Configuration,
            tssDistance: Long,
            biotypes: GeneIndex.BioTypes)(implicit ss: SparkSession): Component = {

    val genes = GeneIndex(conf.ensembl.lut, biotypes).sortByTSS.table
      .selectBy(GeneIndex.columns)
      .cache()

    logger.info("generate nearest dataset from variant annotated index")
    val nearests = vIdx.table
      .select(VariantIndex.columns.head, VariantIndex.columns.tail: _*)
      .withColumn("type_id", lit("distance"))
      .withColumn("source_id", lit("canonical_tss"))
      .withColumn("feature", lit("unspecified"))
      .join(genes,
            (col("chr_id") === col("chr")) and
              (abs(col("position") - col("tss")) <= tssDistance))
      .withColumn("d", abs(col("position") - col("tss")))
      .withColumn("distance_score", when(col("d") > 0, lit(1.0) / col("d")).otherwise(1.0))

    val intWP = nearests
      .transform(computePercentiles(_, "distance_score", "distance_score_q"))

    new Component {

      /** unique column name list per component */
      override val features: Seq[String] = Distance.features
      override val table: DataFrame = intWP
    }
  }
}
