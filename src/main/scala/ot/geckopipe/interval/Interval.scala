package ot.geckopipe.interval

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import ot.geckopipe.Configuration
import ot.geckopipe.index.{EnsemblIndex, VariantIndex}

object Interval extends LazyLogging {
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
      .withColumn("feature", lower(col("feature")))
  }

  def apply(vIdx: VariantIndex, conf: Configuration)(implicit ss: SparkSession): DataFrame = {
    val extractValidTokensFromPath = udf((path: String) => {
      val validTokens = path.split("interval").last.split("/").filter(_.nonEmpty)
      val tokenList = Array(validTokens.head.toLowerCase, validTokens.tail.drop(1).head.toLowerCase)

      tokenList
    })

    val fromRangeToArray = udf((l1: Long, l2: Long) => (l1 to l2).toArray)
    logger.info("load ensembl gene to transcript table, aggregate by gene_id and cache to enrich results")
    val genes = EnsemblIndex(conf.ensembl.geneTranscriptPairs)
      .aggByGene
      .cache

    logger.info("generate pchic dataset from file and aggregating by range and gene")
    val interval = load(conf.interval.path)
      .withColumn("value", array(col("score")))
      .withColumn("tokens", extractValidTokensFromPath(col("filename")))
      .withColumn("source_id", col("tokens").getItem(0))
      .withColumn("tissue_id", col("tokens").getItem(1))
      .drop("filename", "tokens")
      .join(genes, Seq("gene_id"))
      .where(col("chr_id") === col("gene_chr"))
      .withColumn("position", explode(fromRangeToArray(col("position_start"), col("position_end"))))
      .drop("position_start", "position_end", "score")
      .repartitionByRange(col("chr_id").asc, col("position").asc)

    interval.join(vIdx.table, Seq("chr_id", "position"))
  }
}
