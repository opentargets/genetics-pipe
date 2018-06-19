package ot.geckopipe.interval

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode, udf}
import ot.geckopipe.Configuration
import ot.geckopipe.index.VariantIndex

object Interval {
  def features: List[String] = List("pchic", "fantom5", "dhs")
  /** interval columns
    *
    * 1 23456 123 ENSG0000002 promoter [score1, score2, ...]
    */
  val intervalColumnNames: List[String] = List("chr_id", "position_start", "position_end",
    "gene_id", "feature", "value")

  def unwrapInterval(df: DataFrame): DataFrame = {
    val fromRangeToArray = udf((l1: Long, l2: Long) => (l1 to l2).toArray)
    df.withColumn("position", fromRangeToArray(col("position_start"), col("position_end")))
      .withColumn("position", explode(col("position")))
  }

  /** union all intervals and interpolate variants from intervals */
  def buildIntervals(vIdx: VariantIndex, conf: Configuration)
                    (implicit ss: SparkSession): Seq[DataFrame] = {

    val pchic = PCHIC(conf)
    val dhs = DHS(conf)
    val fantom5 = Fantom5(conf)
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
