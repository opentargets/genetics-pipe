package ot.geckopipe

import org.apache.spark.sql.SparkSession

object VEP {
  type GeneTransLUT = Map[String, String]
  // build a map: TissueLUT from a filename with default ("","")
  def buildGeneTransLUT(from: String)(implicit ss: SparkSession): GeneTransLUT = {
    val transcripts = ss.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter","\t")
      .option("mode", "DROPMALFORMED")
      .load(from)

    transcripts.collect
      .map(r => (r.getString(1), r.getString(0)))
      .toMap
  }


}
