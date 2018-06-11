package ot.geckopipe.interval

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.geckopipe.Configuration

object PCHIC extends LazyLogging {
  val schema = StructType(
    StructField("chr_id", StringType) ::
      StructField("position_start", LongType) ::
      StructField("position_end", LongType) ::
      StructField("score", DoubleType) ::
      StructField("gene_id", StringType) :: Nil)

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

    logger.info("generate pchic dataset from file and aggregating by range and gene")
    val pchic = load(conf.interval.pchic)
    val aggPchic = pchic
      .groupBy("chr_id", "position_start", "position_end", "gene_id")
      .agg(collect_list("score").as("value"))
      .withColumn("feature", lit("pchic"))

    aggPchic
  }
}
