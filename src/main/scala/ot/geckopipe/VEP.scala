package ot.geckopipe

import org.apache.spark.sql.{DataFrame, SparkSession}

object VEP {
  def loadGeneTrans(from: String)(implicit ss: SparkSession): DataFrame = {
    val transcripts = ss.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter","\t")
      .option("mode", "DROPMALFORMED")
      .load(from).toDF("gene_id", "trans_id")

    transcripts
  }
}
