package ot.geckopipe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}

object VEP {
  // build the right data schema
  // CHROM POS ID REF ALT QUAL FILTER INFO
  case class VEPRecord(chr: String, pos: Long, rsid: String,
                       refAllele: String, altAllele: String,
                       qual: String, filter: String, csq: List[String])

  val schema: StructType = Encoders.product[VEPRecord].schema

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

  // TODO FIX here!
  def loadHumanVEP(from: String)(implicit ss: SparkSession): DataFrame = {
    def extractCons(from: String): Array[Array[String]] = {
      val csql = from.split(";")
        .filter(_.startsWith("CSQ="))
        .map(_.stripPrefix("CSQ="))
        .map(_.split(","))

      csql
    }

    import ss.implicits._

    val vepsRDD: RDD[VEPRecord] = ss.sparkContext.textFile(from)
      .filter(_.nonEmpty)
      .filter(!_.startsWith("#"))
      .map(_.split("\\s+"))
      .filter(_.length >= 8)
      .flatMap( row => {
        val veps = extractCons(row(7))

        for {
          vep <- veps
        } yield VEPRecord(row(0), row(1).toLong,
            row(2), row(3),
            row(4), row(5),
            row(6), vep.toList)
      })

    val vepsDF = vepsRDD.toDF
    vepsDF.show(10)


    vepsDF.select("chr", "pos", "rsid", "refAllele", "altAllele", "csq")
  }
}
