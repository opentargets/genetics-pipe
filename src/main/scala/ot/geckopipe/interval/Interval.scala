package ot.geckopipe.interval

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode, udf, lit}
import ot.geckopipe.Configuration
import ot.geckopipe.index.VariantIndex
import ot.geckopipe.functions._

object Interval {
  val intervalColumnNames: List[String] = List("chr_id", "position_start", "position_end",
    "gene_id", "feature", "value", "source_id")

  def unwrapInterval(df: DataFrame): DataFrame = {
    val fromRangeToArray = udf((l1: Long, l2: Long) => (l1 to l2).toArray)
    df.withColumn("position", fromRangeToArray(col("position_start"), col("position_end")))
      .withColumn("position", explode(col("position")))
  }

  /** union all intervals and interpolate variants from intervals */
  def buildIntervals(vIdx: VariantIndex, conf: Configuration)
                    (implicit ss: SparkSession): Seq[DataFrame] = {

    val pchic = addSourceID(PCHIC(conf), lit("pchic"))
    val dhs = addSourceID(DHS(conf), lit("dhs"))
    val fantom5 = addSourceID(Fantom5(conf), lit("fantom5"))
    val intervalSeq = Seq(pchic, dhs, fantom5)

    // val fVIdx = vIdx.selectBy(Seq("chr_id", "position", "variant_id"))

    intervalSeq.map(df => {
      val in2Joint = unwrapInterval(df)
        .repartitionByRange(col("chr_id").asc, col("position").asc)

      in2Joint
        .join(vIdx.table, Seq("chr_id", "position"))
        .drop("position_start", "position_end")
    })
  }
}
