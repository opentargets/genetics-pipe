package ot.geckopipe

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object VEP {
//  case class VEPRecord(chr: String, pos: Long, rsid: String,
//                       refAllele: String, altAllele: String,
//                       qual: String, filter: String, csq: List[String], tsa: String)
//
//  val schema: StructType = Encoders.product[VEPRecord].schema

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
      .withColumnRenamed("refAllele", "ref_allele")
      .withColumnRenamed("altAllele", "alt_allele")
      .withColumnRenamed("chr", "chr_name")
      .withColumnRenamed("rsid", "rs_id")
      .withColumnRenamed("pos", "variant_pos")
      .toDF("chr_name", "variant_pos", "rs_id", "ref_allele", "alt_allele", "qual", "filter", "info")
      .withColumn("tsa", udfTSA($"info"))
      .withColumn("csq", udfCSQ($"info"))
      .withColumn("alt_allele",split($"alt_allele", ","))
      .withColumn("alt_allele",explode($"alt_allele"))
      .withColumn("csq", filterCSQByAltAllele($"ref_allele", $"alt_allele", $"tsa", $"csq"))
      .withColumn("csq", explode($"csq"))
      .withColumn("csq", split($"csq", "\\|"))
      .withColumn("consequence", $"csq".getItem(1))
      .withColumn("trans_id", $"csq".getItem(3))
      .drop("qual", "filter", "info", "tsa")
      // .select("chr", "pos", "refAllele", "altAllele", "csq", "consequence", "transID")

    vepss
  }
}
