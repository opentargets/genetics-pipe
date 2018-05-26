package ot.geckopipe

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

object VEP {
//  case class VEPRecord(chr: String, pos: Long, rsid: String,
//                       refAllele: String, altAllele: String,
//                       qual: String, filter: String, csq: List[String], tsa: String)
//
//  val schema: StructType = Encoders.product[VEPRecord].schema

  def loadGeneTrans(from: String)(implicit ss: SparkSession): DataFrame = {
    val transcripts = ss.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter","\t")
      .option("mode", "DROPMALFORMED")
      .load(from).toDF("geneID", "transID")

    transcripts
  }

  def loadHumanVEP(from: String)(implicit ss: SparkSession): DataFrame = {
    // split info string and extract CSQ substring
    // it returns a list of consequences
    val udfCSQ = udf( (info: String) => {
      val csql = info.split(";")
        .filter(_.startsWith("CSQ="))
        .flatMap(_.stripPrefix("CSQ=").split(","))

      csql
    })

    val udfTSA = udf( (info: String) => {
      val tsa = info.split(";")
        .filter(_.startsWith("TSA="))
        .map(_.stripPrefix("TSA="))

      tsa(0)
    })

    val filterCSQByAltAllele = udf( (refAllele: String, altAllele: String, tsa: String, csqs: Seq[String]) => {
      tsa match {
        case s if Set("SNV", "insertion", "substitution").contains(s) =>
          if (altAllele.startsWith(refAllele))
            csqs.filter(_.startsWith(altAllele.stripPrefix(refAllele)))
          else
            csqs.filter(_.startsWith(altAllele))
        case _ =>
          csqs
      }
    })

    import ss.implicits._

    val vepss = ss.read
      .format("csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter","\t")
      .option("comment", "\u0023")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      .option("mode", "DROPMALFORMED")
      .load(from)
      .toDF("chr", "pos", "rsid", "refAllele", "altAllele", "qual", "filter", "info")
      .withColumn("tsa", udfTSA($"info"))
      .withColumn("csq", udfCSQ($"info"))
      .withColumn("altAllele",split($"altAllele", ","))
      .withColumn("altAllele",explode($"altAllele"))
      .withColumn("csq", filterCSQByAltAllele($"refAllele", $"altAllele", $"tsa", $"csq"))
      .withColumn("csq", explode($"csq"))
      .withColumn("csq", split($"csq", "\\|"))
      .withColumn("consequence", $"csq".getItem(1))
      .withColumn("transID", $"csq".getItem(3))
      .drop("qual", "filter", "info", "tsa", "rsid")
      // .select("chr", "pos", "refAllele", "altAllele", "csq", "consequence", "transID")

    vepss
  }
}
