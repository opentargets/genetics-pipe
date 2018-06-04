package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object PCHIC extends LazyLogging {
  def apply(conf: Configuration)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    logger.info("generate pchic dataset from file and aggregating by range and gene")
    val pchic = ss.read
      .format("csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter","\t")
      .option("mode", "DROPMALFORMED")
      //.schema(schema)
      .load(conf.interval.pchic)
      .toDF("chr_id", "position", "interval_end", "score", "gene_id")
      .withColumn("interval_size", abs($"interval_end".minus($"position")))
      .select("chr_id", "position", "interval_size", "score", "gene_id")

    val aggPchic = pchic
      .groupBy("chr_id", "position", "interval_size", "gene_id")
      .agg(collect_list("score").as("score"))
      .persist

    aggPchic
  }
}
