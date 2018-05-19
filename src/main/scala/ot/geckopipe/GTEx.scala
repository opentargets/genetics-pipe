package ot.geckopipe

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.util.Try

object GTEx {
  // build the right data schema
  val schema: StructType = StructType(
    StructField("ld_snp_rsID", StringType) ::
      StructField("chrom", StringType) ::
      StructField("pos", IntegerType) ::
      StructField("GRCh38_chrom", StringType) ::
      StructField("Nearest", DoubleType) ::
      StructField("Regulome", DoubleType) ::
      StructField("VEP_reg", DoubleType) :: Nil
  )

  def loadEGenes(from: String, withSample: Double = .0)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    val loaded = ss.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter","\t")
      .option("mode", "DROPMALFORMED")
      //.schema(schema)
      .load(from)
      .withColumn("filename", input_file_name)

    if (withSample > .0)
      loaded.sample(withReplacement=false, withSample).toDF
    else
      loaded
  }

  def loadVGPairs(from: String, withSample: Double = .0)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    val loaded = ss.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter","\t")
      .option("mode", "DROPMALFORMED")
      //.schema(schema)
      .load(from)
      .withColumn("filename", input_file_name)

    if (withSample > .0)
      loaded.sample(withReplacement=false, withSample).toDF
    else
      loaded
  }

  case class Tissue(code: String, name: String)
  type TissueLUT = Map[String, Tissue]

  // build a map: TissueLUT from a filename with default ("","")
  def buildTissueLUT(from: String)(implicit ss: SparkSession): TissueLUT = {
    val tissueCodes = ss.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter",",")
      .option("mode", "DROPMALFORMED")
      .load(from)

    tissueCodes.collect
      .map(r => (r.getString(0), Tissue(r.getString(2), r.getString(1))))
      .toMap
  }
}
