package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types._

object Dataset extends LazyLogging  {
  def buildGTEx(conf: Configuration)(implicit ss: SparkSession): DataFrame = {
    logger.info(s"build gtex dataframe using map ${conf.gtex.tissueMap}")
    val tissues = GTEx.buildTissue(conf.gtex.tissueMap)

    // TODO still unclear if using egenes or vgpairs or allgenes one
    val vgPairs = GTEx.loadVGPairs(conf.gtex.variantGenePairs)
    val vgPairsWithTissues = vgPairs.join(tissues, Seq("filename"), "left_outer")
        .drop("filename")

    vgPairsWithTissues
  }

  def buildVEP(conf: Configuration)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._
    val geneTrans = VEP.loadGeneTrans(conf.vep.geneTranscriptPairs)
    val veps = VEP.loadHumanVEP(conf.vep.homoSapiensCons)
    val vepsGenes= veps.join(geneTrans,Seq("transID"), "left_outer")
        .withColumn("variant_id",
          concat_ws("_", $"chr", $"pos", $"refAllele", $"altAllele"))

    vepsGenes
  }
}
