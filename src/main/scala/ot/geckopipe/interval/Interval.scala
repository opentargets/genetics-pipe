package ot.geckopipe.interval

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode, udf}
import ot.geckopipe.Configuration
import ot.geckopipe.functions._
import ot.geckopipe.index.VariantIndex

object Interval {
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
                    (implicit ss: SparkSession): Option[DataFrame] = {

    val pchic = PCHIC(conf)
    val dhs = DHS(conf)
    val fantom5 = Fantom5(conf)
    val intervalSeq = Seq(pchic, dhs, fantom5)

    val fVIdx = vIdx.table.select("chr_id", "position", "variant_id", "segment")


    concatDatasets(intervalSeq, Interval.intervalColumnNames) match {
      case None => None
      case Some(dts) =>
        val in2Joint = buildPosSegment(buildPosSegment(dts, "position_start", "segment_start"),
          "position_end", "segment_end")
          .repartitionByRange(col("chr_id").asc, col("position_start").asc)
          // .sort(col("chr_id").asc, col("position_start").asc)

        val jointDts = fVIdx
          .join(in2Joint, fVIdx("position") <= in2Joint("position_end") and fVIdx("position") >= in2Joint("position_start"), "left")
          .where(fVIdx("chr_id") === in2Joint("chr_id") and
            (fVIdx("segment") === in2Joint("segment_start") or fVIdx("segment") === in2Joint("segment_end")))
          .drop("chr_id", "position_start", "position_end", "position", "segment")

        Some(jointDts)
    }
  }
}
