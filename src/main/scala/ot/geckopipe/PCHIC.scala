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
    .toDF("chr", "position", "interval_end", "score", "gene_id")
    .withColumn("interval_size",
      abs(col("interval_end").minus(col("position"))))
    .select("chr", "position", "interval_size", "score", "gene_id")


  def apply(conf: Configuration)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    logger.info("generate pchic dataset from file and aggregating by range and gene")
    val pchic = loadPCHIC(conf.interval.pchic)
    val aggPchic = pchic
      .groupBy("chr", "position", "interval_size", "gene_id")
      .agg(collect_list("score").as("value"))
      .withColumn("source_id", lit("pchic"))
      .withColumn("tissue_id", lit("unknown"))
      .withColumn("feature", lit("promoter"))

    // "gene_id", "source_id", "tissue_id", "feature", "value")
    aggPchic
  }
}
