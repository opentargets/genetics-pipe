package ot.geckopipe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}

object VEP {
  // build the right data schema
  // CHROM POS ID REF ALT QUAL FILTER INFO
  case class VEPRecord(chr: String, pos: Long, rsid: String,
                       refAllele: String, altAllele: String,
                       qual: String, filter: String, csq: List[String], tsa: String)

  val schema: StructType = Encoders.product[VEPRecord].schema

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
    def extractCons(from: String): Array[Array[String]] = {
      val csql = from.split(";")
        .filter(_.startsWith("CSQ="))
        .map(_.stripPrefix("CSQ="))
        .map(_.split(","))

      csql
    }

    def extractTSA(from: String): String = {
      val tsa = from.split(";")
        .filter(_.startsWith("TSA="))
        .map(_.stripPrefix("TSA="))

      tsa(0)
    }

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

    val vepsRDD: RDD[VEPRecord] = ss.sparkContext.textFile(from)
      .filter(_.nonEmpty)
      .filter(!_.startsWith("#"))
      .map(_.split("\\s+"))
      .filter(_.length >= 8)
      .flatMap( row => {
        val veps = extractCons(row(7))
        val tsa = extractTSA(row(7))

        for {
          vep <- veps
        } yield VEPRecord(row(0), row(1).toLong,
          row(2), row(3),
          row(4), row(5),
          row(6), vep.toList,
          tsa)

      })

    val vepsDF = vepsRDD.toDF

    vepsDF.select("chr", "pos", "rsid", "refAllele", "altAllele", "csq", "tsa")
      .withColumn("altAllele",split($"altAllele", ","))
      .withColumn("altAllele",explode($"altAllele"))
      .withColumn("csq", filterCSQByAltAllele($"refAllele", $"altAllele", $"tsa", $"csq"))
      .withColumn("csq", explode($"csq"))
      .withColumn("csq", split($"csq", "\\|"))
      .withColumn("consequence", $"csq".getItem(1))
      .withColumn("transID", $"csq".getItem(3))
  }
}
