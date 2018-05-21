package ot.geckopipe

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.util.Try

object GTEx {
  type TissueLUT = Map[String, Tissue]
  case class Tissue(code: String = "", name: String = "")

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

  def loadEGenes(from: String, withLUT: TissueLUT, withSample: Double = .0)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    val t2c = udf((filename: String) =>
      extractTissueToCode(filename, withLUT))

    val loaded = ss.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter","\t")
      .option("mode", "DROPMALFORMED")
      //.schema(schema)
      .load(from)
      .withColumn("filename", input_file_name)
      .withColumn("tissue_code",
        when($"filename".isNotNull, t2c($"filename"))
          .otherwise(""))


    if (withSample > .0)
      loaded.sample(withReplacement=false, withSample).toDF
    else
      loaded
  }

  def loadVGPairs(from: String, withLUT: TissueLUT, withSample: Double = .0)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    val t2c = udf((filename: String) =>
      extractTissueToCode(filename, withLUT))

    val loaded = ss.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter","\t")
      .option("mode", "DROPMALFORMED")
      //.schema(schema)
      .load(from)
      .withColumn("filename", input_file_name)
      .withColumn("tissue_code",
        when($"filename".isNotNull, t2c($"filename"))
          .otherwise(""))

    if (withSample > .0)
      loaded.sample(withReplacement=false, withSample).toDF
    else
      loaded
  }

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

  private def extractTissueToCode(from: String, lut: TissueLUT): String =
    lut.getOrElse(from.split('/').last,Tissue()).code
}
