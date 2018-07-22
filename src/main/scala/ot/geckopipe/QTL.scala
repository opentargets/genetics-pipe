package ot.geckopipe

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.geckopipe.functions._
import ot.geckopipe.index.V2GIndex.Component
import ot.geckopipe.index.VariantIndex

object QTL extends LazyLogging {
  val features: Seq[String] = Seq("qtl_beta", "qtl_se", "qtl_pval")

  val schema = StructType(
    StructField("chr_id", StringType) ::
      StructField("position", LongType) ::
      StructField("ref_allele", StringType) ::
      StructField("alt_allele", StringType) ::
      StructField("gene_id", StringType) ::
      StructField("beta", DoubleType) ::
      StructField("se", DoubleType) ::
      StructField("pval", DoubleType) :: Nil)

  def load(from: String)(implicit ss: SparkSession): DataFrame = {
    val qtl = ss.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .option("delimiter","\t")
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .load(from)
      .withColumn("filename", input_file_name)

    qtl
  }

  /** union all intervals and interpolate variants from intervals */
  def apply(vIdx: VariantIndex, conf: Configuration)(implicit ss: SparkSession): Component = {
    val extractValidTokensFromPathUDF = udf((path: String) => extractValidTokensFromPath(path, "/qtl/"))

    logger.info("generate pchic dataset from file and aggregating by range and gene")
    val qtls = load(conf.qtl.path)
      .withColumn("tokens", extractValidTokensFromPathUDF(col("filename")))
      .withColumn("type_id", lower(col("tokens").getItem(0)))
      .withColumn("source_id", lower(col("tokens").getItem(1)))
      .withColumn("feature", lower(col("tokens").getItem(2)))
      .withColumnRenamed("beta", "qtl_beta")
      .withColumnRenamed("se", "qtl_se")
      .withColumnRenamed("pval", "qtl_pval")
      .drop("filename", "tokens")
      .repartitionByRange(col("chr_id").asc, col("position").asc)

    val qtlTable = qtls.join(vIdx.table, Seq("chr_id", "position", "ref_allele", "alt_allele"))

    new Component {
      /** unique column name list per component */
      override val features: Seq[String] = QTL.features
      override val table: DataFrame = qtlTable
    }
  }
}
