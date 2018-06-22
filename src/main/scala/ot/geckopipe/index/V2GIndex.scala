package ot.geckopipe.index

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import ot.geckopipe.functions._
import ot.geckopipe.{Chromosomes, Configuration}

// TODO tissue_id should be better represented some how
/** represents a cached table of variants with all variant columns
  *
  * columns as chr_id, position, ref_allele, alt_allele, variant_id, rs_id. Also
  * this table is persisted and sorted by (chr_id, position) by default
  */
abstract class V2GIndex extends LazyLogging with Indexable {
  /** save the dataframe as tsv file using filename as a output path */
  def save(to: String, withFormat: String = "json")(implicit sampleFactor: Double = 0d): Unit = {
    logger.info("write datasets to json lines")
    if (sampleFactor > 0d) {
      table
        .sample(withReplacement = false, sampleFactor)
        .write.format(withFormat)
        .save(to)
    } else {
      table
        .write.format(withFormat)
        .save(to)
    }
  }

  /** compute stats with this resulted table but only when info enabled */
  def computeStats(implicit ss: SparkSession): Seq[Long] = {
    import ss.implicits._
    val totalRows = table.count()
    // val rsidNullsCount = dataset.where($"rsid".isNull).count()
    val inChrCount = table.where($"chr_id".isin(Chromosomes.chrList:_*)).count()

    logger.info(s"count number of rows in chr range $inChrCount of a total $totalRows")
    Seq(inChrCount, totalRows)
  }
}

object V2GIndex extends LazyLogging  {
  /** all data sources to incorporate needs to meet this format at the end
    *
    * One example of the shape of the data could be
    * "1_123_T_C ENSG0000001 gtex uberon_0001 1
    */
  val v2gColumns: Seq[String] = Seq("feature", "value")

  /** columns to index the dataset */
  val indexColumns: Seq[String] = Seq("chr_id", "position")
  /** the whole list of columns this dataset will be outputing */
  val columns: Seq[String] = (VariantIndex.columns ++ EnsemblIndex.columns ++ v2gColumns).distinct

  /** join built gtex and vep together and generate char pos alleles columns from variant_id */
  def build(datasets: Seq[DataFrame], vIdx: VariantIndex, conf: Configuration)(implicit ss: SparkSession): V2GIndex = {

    logger.info("build variant to gene dataset union the list of datasets")
    logger.info("load ensembl gene to transcript table, aggregate by gene_id and cache to enrich results")
    val geneTrans = EnsemblIndex(conf.ensembl.geneTranscriptPairs)
      .aggByGene
      .cache

    val dsMapped = datasets.map(ds => {
      // ds.groupBy("variant_id", "gene_id")
      // .pivot("feature", features)
      // .agg(first(col("value")))
      ds.where(col("chr_id") equalTo col("gene_chr"))
        .join(geneTrans, Seq("gene_id"))
    })
    // concatDatasets(datasets, v2gColumnNames.take(2) ++ features)
    val allDts = concatDatasets(dsMapped, columns)

    new V2GIndex {
      /** uniform way to get the dataframe */
      override val table: DataFrame = allDts
    }
  }

  /** join built gtex and vep together and generate char pos alleles columns from variant_id */
  def load(conf: Configuration, withFormat: String = "json")(implicit ss: SparkSession): V2GIndex = {

    logger.info("load variant to gene dataset from built one")
    val v2g = ss.read
      .format("json")
      .load(conf.variantGene.path)

    new V2GIndex {
      /** uniform way to get the dataframe */
      override val table: DataFrame = v2g
    }
  }
}
