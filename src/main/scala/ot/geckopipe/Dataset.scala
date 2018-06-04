package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

object Dataset extends LazyLogging  {
  /** join built gtex and vep together and generate char pos alleles columns from variant_id */
  // def buildV2G(gtex: DataFrame, vep: DataFrame, conf: Configuration)(implicit ss: SparkSession): DataFrame = {
  def buildV2G(datasets: Seq[DataFrame], conf: Configuration)(implicit ss: SparkSession): Option[DataFrame] = {
    import ss.implicits._

    if (datasets.nonEmpty) {
      logger.info("build variant to gene dataset union the list of datasets")
      val dts = datasets.foldLeft(datasets.head.select(columnNames.head, columnNames.tail:_*))((aggDt, dt) => {
        aggDt.union(dt.select(columnNames.head, columnNames.tail:_*))
      })

      logger.info("load ensembl gene to transcript table, aggregate by gene_id and cache to enrich results")
      val geneTrans = Ensembl.loadEnsemblG2T(conf.ensembl.geneTranscriptPairs)
        .select("gene_id", "gene_start", "gene_end", "gene_chr", "gene_name", "gene_type")
        .groupBy("gene_id")
        .agg(first($"gene_start").as("gene_start"),
          first($"gene_end").as("gene_end"),
          first($"gene_chr").as("gene_chr"),
          first($"gene_name").as("gene_name"),
          first($"gene_type").as("gene_type"))
        .cache

      logger.info("split variant_id info into pos ref allele and alt allele")
      val dtsEnriched = dts
        .withColumn("_tmp", split($"variant_id", "_"))
        // .withColumn("chr_name", $"_tmp".getItem(0))
        .withColumn("variant_pos", $"_tmp".getItem(1).cast(LongType))
        .withColumn("ref_allele", $"_tmp".getItem(2))
        .withColumn("alt_allele", $"_tmp".getItem(3))
        .drop("_tmp")

      logger.info("enrich union datasets with gene info")
      val v2gEnriched = dtsEnriched.join(geneTrans, Seq("gene_id"), "left_outer")

      Some(v2gEnriched)

    } else {
      None
    }
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
    logger.info("write datasets to json lines")
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
