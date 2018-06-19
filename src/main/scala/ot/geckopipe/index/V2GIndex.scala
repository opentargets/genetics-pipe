package ot.geckopipe.index

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.geckopipe.functions._
import ot.geckopipe.{Chromosomes, Configuration}

object V2GIndex extends LazyLogging  {
  /** all data sources to incorporate needs to meet this format at the end
    *
    * One example of the shape of the data could be
    * "1_123_T_C ENSG0000001 gtex uberon_0001 1
    */
  val v2gColumnNames: List[String] = List("variant_id", "gene_id", "feature", "value")

  /** join built gtex and vep together and generate char pos alleles columns from variant_id */
  def apply(datasets: Seq[DataFrame], vIdx: VariantIndex, conf: Configuration)(implicit ss: SparkSession): Option[DataFrame] = {

    logger.info("build variant to gene dataset union the list of datasets")
    logger.info("load ensembl gene to transcript table, aggregate by gene_id and cache to enrich results")
    val geneTrans = EnsemblIndex(conf.ensembl.geneTranscriptPairs)
      .aggByGene
      .cache

    // val features = Positional.features(conf) ++ Interval.features
    val dsMapped = datasets.map(ds => {
      // ds.groupBy("variant_id", "gene_id")
        // .pivot("feature", features)
        // .agg(first(col("value")))
      ds.join(geneTrans, Seq("gene_id"))
        .join(vIdx.table, Seq("variant_id"))
    })
    // concatDatasets(datasets, v2gColumnNames.take(2) ++ features)
    concatDatasets(dsMapped, v2gColumnNames)
  }

  /** compute stats with this resulted table but only when info enabled */
  def computeStats(dataset: DataFrame, tableName: String)(implicit ss: SparkSession): Seq[Long] = {
    import ss.implicits._
    val totalRows = dataset.count()
    // val rsidNullsCount = dataset.where($"rsid".isNull).count()
    val inChrCount = dataset.where($"chr_id".isin(Chromosomes.chrList:_*)).count()

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
        .save(filename)
    } else {
      dataset
        .write.format("json")
        .save(filename)
    }
  }
}
