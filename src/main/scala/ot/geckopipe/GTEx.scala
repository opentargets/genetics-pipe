package ot.geckopipe

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

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

  def load(from: String, withSample: Double = .0)(implicit ss: SparkSession): DataFrame = {
    val loaded = ss.read
      .format("csv")
      .option("header", "true")
      // .option("inferSchema", "true")
      .option("delimiter","\t")
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .load(from)

    if (withSample > .0)
      loaded.sample(withReplacement=false, withSample).toDF
    else
      loaded
  }
}
