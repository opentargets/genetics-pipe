package ot.geckopipe.positional

import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.geckopipe.Configuration
import ot.geckopipe.index.VariantIndex

object Positional {
  def features(conf: Configuration)(implicit ss: SparkSession): List[String] = {
    val csqs = VEP.loadConsequenceTable(conf.vep.csq).select("so_term").collect.map(_.getString(0)).toList
    val tissues = GTEx.buildTissue(conf.gtex.tissueMap).select("uberon_code").collect.map(_.getString(0)).toList

    csqs ++ tissues
  }

  /** variant_id is represented as 1_123_T_C but splitted into columns 1 23456 T C */
  val variantColumnNames: List[String] = List("chr_id", "position", "ref_allele", "alt_allele")
  /** types of the columns named in variantColumnNames */
  val variantColumnTypes: List[String] = List("String", "long", "string", "string")

  /** union all intervals and interpolate variants from intervals */
  def buildPositionals(vIdx: VariantIndex, conf: Configuration)
                      (implicit ss: SparkSession): Seq[DataFrame] = {

    val gtex = GTEx(conf)
    val vep = VEP(conf)
    Seq(gtex, vep)
  }
}
