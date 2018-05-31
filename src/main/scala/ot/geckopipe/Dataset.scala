package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
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
    * generate pivot per consecuence and set to count or fill with 0
    */
  def buildVEP(conf: Configuration)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    val geneTrans = Ensembl.loadEnsemblG2T(conf.ensembl.geneTranscriptPairs)
      .select("gene_id", "trans_id")

    val vepCsqs = VEP.loadConsequenceTable(conf.vep.csq)
      .select("so_term")
      .collect.toList.map(row => row(0).toString).sorted

    val veps = VEP.loadHumanVEP(conf.vep.homoSapiensCons)

    val vepsDF = veps.join(geneTrans, Seq("trans_id"))
      .withColumn("variant_id",
        concat_ws("_", $"chr_name", $"variant_pos", $"ref_allele", $"alt_allele"))
      .drop("trans_id", "csq", "chr_name", "variant_pos", "ref_allele", "alt_allele")
      .where($"gene_id".isNotNull)

    val vepsDFF = vepsDF
      .groupBy("variant_id", "gene_id")
      .agg(collect_set($"consequence").as("consequence_set"),
        first($"rs_id").as("rs_id"))

    val vepsPivot = vepsDF
      .select("variant_id", "gene_id", "consequence")
      .groupBy("variant_id", "gene_id")
      .pivot("consequence", vepCsqs)
      .count
      .drop("consequence")
      .na.fill(0L)

    vepsDFF.join(vepsPivot, Seq("variant_id", "gene_id"))
  }

  /** join built gtex and vep together and generate char pos alleles columns from variant_id */
  def buildV2G(gtex: DataFrame, vep: DataFrame, conf: Configuration)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    val geneTrans = Ensembl.loadEnsemblG2T(conf.ensembl.geneTranscriptPairs)
      .select("gene_id", "gene_start", "gene_end", "gene_chr", "gene_name", "gene_type")

    val v2g = vep.join(gtex, Seq("variant_id", "gene_id"), "full_outer")
      .withColumn("_tmp", split($"variant_id", "_"))
      // .withColumn("chr_name", $"_tmp".getItem(0))
      .withColumn("variant_pos", $"_tmp".getItem(1).cast(LongType))
      .withColumn("ref_allele", $"_tmp".getItem(2))
      .withColumn("alt_allele", $"_tmp".getItem(3))
      .drop("_tmp")

    val v2gEnriched = v2g.join(geneTrans, Seq("gene_id"))
      .persist // persist to use with the tempview

    v2gEnriched
  }

  /** compute stats with this resulted table but only when info enabled */
  def computeStats(dataset: DataFrame, tableName: String)(implicit ss: SparkSession): Seq[Long] = {
    import ss.implicits._
    val totalRows = dataset.count()
    // val rsidNullsCount = dataset.where($"rsid".isNull).count()
    val inChrCount = dataset.where($"chr_name".isin(Chromosomes.chrList:_*)).count()

    logger.info(s"count number of rows in chr range $inChrCount of a total $totalRows")
    Seq(inChrCount, totalRows)
  }

  /** save the dataframe as tsv file using filename as a output path */
  def saveToFile(dataset: DataFrame, filename: String)(implicit sampleFactor: Double = 0d): Unit = {
    if (sampleFactor > 0d) {
      dataset
        .sample(withReplacement = false, sampleFactor)
        .write.format("json")
        // .option("sep", "\t")
        // .option("header", "true")
        .save(filename)
    } else {
      dataset
        .write.format("json")
        // .option("sep", "\t")
        // .option("header", "true")
        .save(filename)
    }
  }
}
