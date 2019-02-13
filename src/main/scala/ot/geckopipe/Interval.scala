package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.geckopipe.functions._
import ot.geckopipe.index.V2GIndex.Component
import ot.geckopipe.index.{GeneIndex, VariantIndex}
import ot.geckopipe.index.Indexable._

object Interval extends LazyLogging {
  val features: Seq[String] = Seq("interval_score")

  case class IntervalRow(chrom: String, start: Long, end: Long, gene_id: String, bio_feature: String)

  val schema = StructType(
    StructField("chr_id", StringType) ::
      StructField("position_start", LongType) ::
      StructField("position_end", LongType) ::
      StructField("gene_id", StringType) ::
      StructField("score", DoubleType) ::
      StructField("feature", StringType) :: Nil)

  def load(from: String)(implicit ss: SparkSession): DataFrame = {
    ss.read
      .parquet(from)
      .withColumn("filename", input_file_name)
      .withColumnRenamed("bio_feature", "feature")
  }

  def apply(vIdx: VariantIndex, conf: Configuration)(implicit ss: SparkSession): Component = {
    import ss.implicits._

    val extractValidTokensFromPathUDF = udf((path: String) => extractValidTokensFromPath(path, "/interval/"))
    val fromRangeToArray = udf((l1: Long, l2: Long) => (l1 to l2).toArray)
    logger.info("load ensembl gene table and cache to enrich results")
    val genes = GeneIndex(conf.ensembl.lut)
      .sortByID
      .table.selectBy(GeneIndex.indexColumns :+ "gene_id")
      .withColumnRenamed("chr", "chr_id")
      .cache()

    logger.info("generate pchic dataset from file and aggregating by range and gene")
    val interval = load(conf.interval.path)
      .withColumn("tokens", extractValidTokensFromPathUDF(col("filename")))
      .withColumn("type_id", lower(col("tokens").getItem(0)))
      .withColumn("source_id", lower(col("tokens").getItem(1)))
      .withColumnRenamed("chrom", "chr_id")
      .drop("filename", "tokens")
      .join(genes, Seq("chr_id", "gene_id"))
      .groupBy("chr_id", "start", "end", "gene_id", "type_id", "source_id", "feature")
      .agg(max(col("score")).as("interval_score"))
      .withColumn("position", explode(fromRangeToArray(col("start"), col("end"))))
      .drop("start", "end", "score")
      .repartitionByRange(col("chr_id").asc, col("position").asc)
      .sortWithinPartitions(col("chr_id").asc, col("position").asc)

    interval.show(false)

    val inTable = interval.join(vIdx.table, VariantIndex.indexColumns)

    new Component {
      /** unique column name list per component */
      override val features: Seq[String] = Interval.features
      override val table: DataFrame = inTable
    }
  }
}
