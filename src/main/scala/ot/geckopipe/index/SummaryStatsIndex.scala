package ot.geckopipe.index

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.geckopipe.Configuration

abstract class SummaryStatsIndex extends Indexable

object SummaryStatsIndex extends LazyLogging {
  val columns: Seq[String] = Seq("rd_id", "variant_id", "chr_id", "position", "ref_allele", "alt_allele",
    "eaf", "beta", "se", "pval", "n", "n_cases", "stid")

  val indexColumns: Seq[String] = Seq("chr_id", "position")

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
      StructField("n_cases", StringType) :: Nil)

  def load(vIdx: VariantIndex, conf: Configuration)(implicit ss: SparkSession): SummaryStatsIndex = {
    val buildStudyID = udf((filename: String) => {
      "NEALEUKB_" + filename.split("/").last.split("\\.").head
    })

    val toMinDouble = udf((value: Double) => {
      value match {
        case Double.PositiveInfinity => Double.MaxValue
        case Double.NegativeInfinity => Double.MinValue
        case 0.0 => Double.MinPositiveValue
        case -0.0 => -Double.MinPositiveValue
        case _ => value
      }
    })

    val sa = ss.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .option("delimiter","\t")
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .load(conf.summaryStats.studies)
      .withColumn("filename", input_file_name)
      .withColumn("stid", buildStudyID(col("filename")))
      .drop("filename", "variant_id", "rs_id")
      .withColumn("n_cases", when(col("n_cases").equalTo("nan"), lit(null)).cast(IntegerType))
      .withColumn("pval", toMinDouble(col("pval")))

    sa.join(vIdx.table, Seq("chr_id", "position", "ref_allele", "alt_allele"))

    new SummaryStatsIndex {
      /** uniform way to get the dataframe */
      override val table: DataFrame = sa
    }
  }
}
