package ot.geckopipe

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object GTEx {
  def loadVGPairs(from: String)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    val f2t = udf((filename: String) =>
      extractFilename(filename))

    val removeBuild = udf((variantID: String) =>
      variantID.stripSuffix("_b37"))

    val cleanGeneID = udf((geneID: String) => {
      if (geneID.nonEmpty && geneID.contains("."))
        geneID.split("\\.")(0)
      else
        geneID
    })

    val loaded = ss.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter","\t")
      .option("mode", "DROPMALFORMED")
      //.schema(schema)
      .load(from)
      .withColumn("filename", input_file_name)
      .withColumn("filename",
        when($"filename".isNotNull, f2t($"filename"))
          .otherwise(""))
      .withColumn("gene_id",
        when($"gene_id".contains("."),cleanGeneID($"gene_id"))
      )
      .withColumn("variant_id", removeBuild($"variant_id"))

    loaded
  }

  // build a map: TissueLUT from a filename with default ("","")
  def buildTissue(from: String)(implicit ss: SparkSession): DataFrame = {
    val tissueCodes = ss.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter",",")
      .option("mode", "DROPMALFORMED")
      .load(from)

    tissueCodes
  }

  private def extractFilename(from: String): String = from.split('/').last
}
