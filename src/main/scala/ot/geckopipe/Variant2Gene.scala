package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import ot.geckopipe.index.{EnsemblIndex, VariantIndex}
import ot.geckopipe.positional._
import ot.geckopipe.interval._

import scala.util.{Failure, Success}

object Variant2Gene extends LazyLogging  {
  /** all data sources to incorporate needs to meet this format at the end
    *
    * One example of the shape of the data could be
    * "1_123_T_C ENSG0000001 gtex uberon_0001 1
    */
  val v2gColumnNames: List[String] = List("variant_id", "gene_id", "source_id", "tissue_id",
    "feature", "value")

  def concatDatasets(datasets: Seq[DataFrame], columns: List[String]): Option[DataFrame] = datasets match {
    case Nil => None
    case _ =>
      logger.info("build variant to gene dataset union the list of datasets")
      val dts = datasets.foldLeft(datasets.head.select(columns.head, columns.tail: _*))((aggDt, dt) => {
        aggDt.union(dt.select(columns.head, columns.tail: _*))
      })

      Some(dts)
  }

  /** union all intervals and interpolate variants from intervals */
  def buildIntervals(vIdx: VariantIndex, intervals: Seq[DataFrame], conf: Configuration)
                    (implicit ss: SparkSession): Option[DataFrame] = {

    concatDatasets(intervals, intervalColumnNames) match {
      case None => None
      case Some(in2) =>
        val fVIdx = vIdx.table.select("chr_id", "position", "variant_id")
        val in2Joint = Functions.unwrapInterval(in2)

        val integratedData = fVIdx
          .join(in2Joint, Seq("chr_id", "position"), "inner")
          .drop("chr_id", "position_start", "position_end", "position")

        Some(integratedData)
    }
  }

  /** join built gtex and vep together and generate char pos alleles columns from variant_id */
  // def buildV2G(gtex: DataFrame, vep: DataFrame, conf: Configuration)(implicit ss: SparkSession): DataFrame = {
  def apply(datasets: Seq[DataFrame], vIdx: VariantIndex, conf: Configuration)(implicit ss: SparkSession): Option[DataFrame] = {
    import ss.implicits._

    concatDatasets(datasets, v2gColumnNames) match {
      case None => None
      case Some(dts) =>
        logger.info("build variant to gene dataset union the list of datasets")
        logger.info("load ensembl gene to transcript table, aggregate by gene_id and cache to enrich results")
        val geneTrans = EnsemblIndex(conf.ensembl.geneTranscriptPairs)
          .aggByGene
          .cache

        logger.info("split variant_id info into pos ref allele and alt allele")
        logger.warn("NOT enrich union datasets with gene info")
        // List("chr_id", "position", "ref_allele", "alt_allele")
        val varid = vIdx.table.select("variant_id", "rs_id")
        val dtsEnriched = Functions.splitVariantID(dts).map(
          _.join(geneTrans, Seq("gene_id"), "left_outer")
            .join(varid, Seq("variant_id"), "left_outer")
        )

        dtsEnriched match {
          case Success(df) => Some(df)
          case Failure(e) =>
            logger.error(e.toString)
            None
        }
    }
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
