package ot.geckopipe.positional

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import ot.geckopipe.Configuration
import ot.geckopipe.index.VariantIndex
import ot.geckopipe.functions._

object Positional {
  def features(conf: Configuration)(implicit ss: SparkSession): List[String] = {
    val csqs = VEP.loadConsequenceTable(conf.vep.csq).select("so_term").collect.map(_.getString(0)).toList
    val tissues = GTEx.buildTissue(conf.gtex.tissueMap).select("uberon_code").collect.map(_.getString(0)).toList

    csqs ++ tissues
  }

  /** union all intervals and interpolate variants from intervals */
  def buildPositionals(vIdx: VariantIndex, conf: Configuration)
                      (implicit ss: SparkSession): Seq[DataFrame] = {

    val gtex = addSourceID(GTEx(conf), lit("gtex")).join(vIdx.table, Seq("variant_id"))
    val vep = addSourceID(VEP(conf), lit("vep"))
    Seq(gtex, vep)
  }
}
