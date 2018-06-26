package ot.geckopipe.qtl

import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.geckopipe.Configuration
import ot.geckopipe.index.VariantIndex

object QTL {
  def features(conf: Configuration)(implicit ss: SparkSession): List[String] = {
    val csqs = VEP.loadConsequenceTable(conf.vep.csqMap).select("so_term").collect.map(_.getString(0)).toList
    val tissues = GTEx.buildTissue(conf.gtex.tissueMap).select("uberon_code").collect.map(_.getString(0)).toList

    csqs ++ tissues
  }

  /** union all intervals and interpolate variants from intervals */
  def apply(vIdx: VariantIndex, conf: Configuration)(implicit ss: SparkSession): Seq[DataFrame] = {

    val gtex = GTEx(conf).join(vIdx.table, Seq("variant_id"))
    val vep = VEP(conf)
    Seq(gtex, vep)
  }
}
