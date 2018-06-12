package ot.geckopipe.interval

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.geckopipe.Configuration
import ot.geckopipe.index.EnsemblIndex

object Fantom5 extends LazyLogging {
  val schema = StructType(
    StructField("chr_id", StringType) ::
      StructField("position_start", LongType) ::
      StructField("position_end", LongType) ::
      StructField("gene_name", StringType) ::
      StructField("score", DoubleType) :: Nil)

  def load(from: String)(implicit ss: SparkSession): DataFrame = {

    ss.read
      .format("csv")
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter","\t")
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .load(from)
  }

  def apply(conf: Configuration)(implicit ss: SparkSession): DataFrame = {
    def _transGene(df: DataFrame): DataFrame = {
      val geneT = EnsemblIndex(conf.ensembl.geneTranscriptPairs)
        .aggByGene
        .select("gene_id", "gene_name")
        .cache

      df.join(geneT, Seq("gene_name"))
        .drop("gene_name")
    }

    logger.info("generate Fantom5 dataset from file and aggregating by range and gene")
    val f5 = load(conf.interval.fantom5)
    val aggF5= f5
      .groupBy("chr_id", "position_start", "position_end", "gene_name")
      .agg(collect_list("score").as("value"))
      .withColumn("feature", lit("fantom5"))

    _transGene(aggF5)
  }
}
