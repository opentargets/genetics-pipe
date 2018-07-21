package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SummaryStats extends LazyLogging {
  val schema = StructType(
    StructField("rs_id", StringType) ::
      StructField("variant_id", StringType) ::
      StructField("chr_id", StringType) ::
      StructField("position", LongType) ::
      StructField("ref_allele", StringType) ::
      StructField("alt_allele", StringType) ::
      StructField("eaf", DoubleType) ::
      StructField("beta", DoubleType) ::
      StructField("se", DoubleType) ::
      StructField("pval", DoubleType) ::
      StructField("n", LongType) ::
      StructField("n_cases", LongType) :: Nil)

  def load(from: String)(implicit ss: SparkSession): DataFrame = {
    val buildStudyID = udf((filename: String) => {
      "NEALEUKB_" + filename.split("/").last.split(".").head
    })

    val sa = ss.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .option("delimiter","\t")
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .load(from)
      .withColumn("filename", input_file_name)
      .withColumn("stid", buildStudyID(col("filename")))
      .drop("filename")

    sa
  }
}
