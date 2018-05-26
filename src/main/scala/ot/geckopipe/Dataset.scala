package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._

object Dataset extends LazyLogging  {

  /** build gtex dataset using variant gene map and pivot all tissues */
  def buildGTEx(conf: Configuration)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    logger.info(s"build gtex dataframe using map ${conf.gtex.tissueMap}")
    val tissues = GTEx.buildTissue(conf.gtex.tissueMap)
    val vgPairs = GTEx.loadVGPairs(conf.gtex.variantGenePairs)

    vgPairs.join(tissues, Seq("filename"), "left_outer")
      .withColumnRenamed("uberon_code", "tissue_code")
      .drop("filename", "gtex_tissue", "ma_samples",
        "ma_count", "maf", "pval_nominal_threshold", "min_pval_nominal")
      .repartition($"variant_id", $"gene_id")
      .persist
  }

  /** join gene id per extracted transcript (it should be one per row)
    *
    * generate variant_id column
    * drop not needed ones
    * rename geneID to gene_id in order to keep names equal
    * filter out those with no gene_id
    * repartition based on variant_id and gene_id
    * and persist if you want to keep the partition through next operations (by ex. joins)
    */
  def buildVEP(conf: Configuration)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    val geneTrans = VEP.loadGeneTrans(conf.vep.geneTranscriptPairs)
    val veps = VEP.loadHumanVEP(conf.vep.homoSapiensCons)
    veps.join(geneTrans,Seq("transID"), "left_outer")
      .withColumn("variant_id",
        concat_ws("_", $"chr", $"pos", $"refAllele", $"altAllele"))
      .drop("transID", "csq", "chr", "pos", "refAllele", "altAllele")
      .withColumnRenamed("geneID", "gene_id")
      .where($"gene_id".isNotNull)
      .groupBy("variant_id", "gene_id")
      .agg(collect_set("consequence"))
      .repartition($"variant_id", $"gene_id")
      .persist
  }

  /** join built gtex and vep together and generate char pos alleles columns from variant_id */
  def joinGTExAndVEP(gtex: DataFrame, vep: DataFrame)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    vep.join(gtex, Seq("variant_id", "gene_id"), "full_outer")
      .withColumn("_tmp", split($"variant_id", "_"))
      .withColumn("chr", $"_tmp".getItem(0))
      .withColumn("pos", $"_tmp".getItem(1).cast(LongType))
      .withColumn("ref_allele", $"_tmp".getItem(2))
      .withColumn("alt_allele", $"_tmp".getItem(3))
      .drop("_tmp")
      .persist // persist to use with the tempview
  }

  /** compute stats with this resulted table but only when info enabled */
  def computeStats(dataset: DataFrame, tableName: String)(implicit ss: SparkSession): Unit = {
    import ss.implicits._
    // persist the created table
    logger.whenInfoEnabled {
      val totalRows = dataset.count()
      // val rsidNullsCount = dataset.where($"rsid".isNull).count()
      val inChrCount = dataset.where($"chr".isin(Chromosomes.chrList:_*)).count()

      logger.info(s"count number of rows in chr range $inChrCount of a total $totalRows")
    }
  }

  /** save the dataframe as tsv file using filename as a output path */
  def saveToFile(dataset: DataFrame, filename: String)(implicit sampleFactor: Double = 0d): Unit = {
    if (sampleFactor > 0d) {
      dataset
        .sample(withReplacement = false, sampleFactor)
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
