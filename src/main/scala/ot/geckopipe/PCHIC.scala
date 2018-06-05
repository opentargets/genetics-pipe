package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object PCHIC extends LazyLogging {

  def loadPCHIC(from: String)(implicit ss: SparkSession): DataFrame = ss.read
    .format("csv")
    .option("header", "false")
    .option("inferSchema", "true")
    .option("delimiter","\t")
    .option("mode", "DROPMALFORMED")
    //.schema(schema)
    .load(from)
    .toDF("chr_id", "position_start", "position_end", "score", "gene_id")

  def apply(conf: Configuration)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    logger.info("generate pchic dataset from file and aggregating by range and gene")
    val pchic = loadPCHIC(conf.interval.pchic)
    val aggPchic = pchic
      .groupBy("chr_id", "position_start", "position_end", "gene_id")
      .agg(collect_list("score").as("value"))
      .withColumn("source_id", lit("pchic"))
      .withColumn("tissue_id", lit("unknown"))
      .withColumn("feature", lit("promoter"))

    aggPchic
  }
}
