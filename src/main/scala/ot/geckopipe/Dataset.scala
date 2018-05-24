package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types._

object Dataset extends LazyLogging  {
  def buildGTEx(conf: Configuration)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    logger.info(s"build gtex dataframe using map ${conf.gtex.tissueMap}")
    val tissues = GTEx.buildTissue(conf.gtex.tissueMap)
    val tissueList = tissues.collect.map(r => r(2)).toList

    // TODO still unclear if using egenes or vgpairs or allgenes one
    val vgPairs = GTEx.loadVGPairs(conf.gtex.variantGenePairs)
    val vgPairsWithTissues = vgPairs.join(tissues, Seq("filename"), "left_outer")
      .withColumnRenamed("uberon_code", "tissue_code")
      .drop("filename", "gtex_tissue", "ma_samples",
        "ma_count", "maf", "pval_nominal_threshold", "min_pval_nominal")
      .repartition($"variant_id", $"gene_id").persist

    val vgPivot = vgPairsWithTissues
      .groupBy("gene_id", "variant_id")
      .pivot("tissue_code", tissueList)
      .count()
      .repartition($"variant_id", $"gene_id").persist

    vgPairsWithTissues.join(vgPivot, Seq("gene_id", "variant_id"), "left_outer")
      .drop("tissue_code")
      .na.fill(0.0)
      .repartition($"variant_id", $"gene_id").persist
  }

  def buildVEP(conf: Configuration)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._
    val geneTrans = VEP.loadGeneTrans(conf.vep.geneTranscriptPairs)
    val veps = VEP.loadHumanVEP(conf.vep.homoSapiensCons)
    val vepsGenes= veps.join(geneTrans,Seq("transID"), "left_outer")
        .withColumn("variant_id",
          concat_ws("_", $"chr", $"pos", $"refAllele", $"altAllele"))
        .drop("transID")
        .drop("csq")
        .withColumnRenamed("geneID", "gene_id")

    vepsGenes
      .repartition($"variant_id", $"gene_id")
      .persist
  }

  def joinGTExAndVEP(gtex: DataFrame, vep: DataFrame): DataFrame = {
    vep.join(gtex, Seq("variant_id", "gene_id"), "full_outer")
  }

  def saveToFile(dataset: DataFrame, filename: String)(implicit sampleFactor: Double = 0d): Unit = {
    if (sampleFactor > 0d) {
      dataset
        .sample(withReplacement = false, c.sampleFactor)
        .write.format("csv")
        .option("sep", "\t")
        .option("header", "true")
        .save(filename)
    } else {
      dataset
        .write.format("csv")
        .option("sep", "\t")
        .option("header", "true")
        .save(filename)
    }
  }
}
