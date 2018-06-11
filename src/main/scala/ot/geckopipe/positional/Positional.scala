package ot.geckopipe.positional

import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.geckopipe.Configuration
import ot.geckopipe.functions.concatDatasets
import ot.geckopipe.index.V2GIndex.v2gColumnNames
import ot.geckopipe.index.VariantIndex

object Positional {
  /** variant_id is represented as 1_123_T_C but splitted into columns 1 23456 T C */
  val variantColumnNames: List[String] = List("chr_id", "position", "ref_allele", "alt_allele")
  /** types of the columns named in variantColumnNames */
  val variantColumnTypes: List[String] = List("String", "long", "string", "string")

  /** union all intervals and interpolate variants from intervals */
  def buildPositionals(vIdx: VariantIndex, conf: Configuration)
                      (implicit ss: SparkSession): Option[DataFrame] = {

    val gtex = GTEx(conf)
    val vep = VEP(conf)
    val positionalSeq = Seq(gtex, vep)
    concatDatasets(positionalSeq, v2gColumnNames)
  }
}
