package ot.geckopipe.interval

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import ot.geckopipe.Configuration
import ot.geckopipe.functions._
import ot.geckopipe.index.V2GIndex.Component
import ot.geckopipe.index.{EnsemblIndex, VariantIndex}

object Interval extends LazyLogging {
  val features: Seq[String] = Seq("interval_score")

  val schema = StructType(
    StructField("chr_id", StringType) ::
      StructField("position_start", LongType) ::
      StructField("position_end", LongType) ::
      StructField("gene_id", StringType) ::
      StructField("score", DoubleType) ::
      StructField("feature", StringType) :: Nil)

  def load(from: String)(implicit ss: SparkSession): DataFrame = {
    ss.read
      .format("csv")
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter","\t")
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .load(from)
      .withColumn("filename", input_file_name)
      .withColumn("feature", col("feature"))
  }

  def apply(vIdx: VariantIndex, conf: Configuration)(implicit ss: SparkSession): Component = {
    val extractValidTokensFromPathUDF = udf((path: String) => extractValidTokensFromPath(path, "/interval/"))

    val fromRangeToArray = udf((l1: Long, l2: Long) => (l1 to l2).toArray)
    logger.info("load ensembl gene to transcript table, aggregate by gene_id and cache to enrich results")
    val genes = EnsemblIndex(conf.ensembl.geneTranscriptPairs)
      .aggByGene
      .select("gene_id", "gene_chr")
      .cache

    logger.info("generate pchic dataset from file and aggregating by range and gene")
    val interval = load(conf.interval.path)
      .withColumn("tokens", extractValidTokensFromPathUDF(col("filename")))
      .withColumn("type_id", lower(col("tokens").getItem(0)))
      .withColumn("source_id", lower(col("tokens").getItem(1)))
      .withColumn("feature", lower(col("tokens").getItem(2)))
      .drop("filename", "tokens")
      .join(genes, Seq("gene_id"))
      .where(col("chr_id") === col("gene_chr"))
      .drop("gene_chr")
      .groupBy("chr_id", "position_start", "position_end", "gene_id", "type_id", "source_id", "feature")
      .agg(max(col("score")).as("interval_score"))
      .withColumn("position", explode(fromRangeToArray(col("position_start"), col("position_end"))))
      .drop("position_start", "position_end", "score")
      .repartitionByRange(col("chr_id").asc, col("position").asc)

    val inTable = interval.join(vIdx.table, Seq("chr_id", "position"))

    new Component {
      /** unique column name list per component */
      override val features: Seq[String] = Interval.features
      override val table: DataFrame = inTable
    }
  }
}
