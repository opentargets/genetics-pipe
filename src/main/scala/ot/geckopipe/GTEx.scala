package ot.geckopipe

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

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

  // Represents a map of a (tissue name, tissueID)
  type TissueLUT = Map[String, String]

  // build a map: TissueLUT from a filename with default ("","")
  def buildTissueLUT(filename: String)(implicit ss: SparkSession): TissueLUT = {
    val tissueCodes = ss.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter","\t")
      .option("mode", "DROPMALFORMED")
      .load(filename)

    tissueCodes.collect
      .map(r => (r.getString(1), r.getString(0)))
      .toMap withDefaultValue("")
  }
}
