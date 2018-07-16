package ot.geckopipe.index

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import ot.geckopipe.Configuration

abstract class V2DIndex extends Indexable {

}

object V2DIndex extends LazyLogging  {
  val studiesSchema = StructType(
    StructField("stid", StringType) ::
      StructField("pmid", StringType) ::
      StructField("pub_date", StringType) ::
      StructField("pub_journal", StringType) ::
      StructField("pub_title", StringType) ::
      StructField("pub_author", StringType) ::
      StructField("trait_reported", StringType) ::
      StructField("trait_mapped", StringType) ::
      StructField("trait_efos", StringType) ::
      StructField("ancestry_initial", StringType) ::
      StructField("ancestry_replication", StringType) ::
      StructField("n_initial", LongType) ::
      StructField("n_replication", LongType) :: Nil)

  val topLociSchema = StructType(
    StructField("stid", StringType) ::
      StructField("variant_id", StringType) ::
      StructField("rs_id", StringType) ::
      StructField("pval_mantissa", DoubleType) ::
      StructField("pval_exponent", DoubleType) :: Nil)

  val ldSchema = StructType(
    StructField("stid", StringType) ::
      StructField("index_variant_id", StringType) ::
      StructField("tag_variant_id", StringType) ::
      StructField("r2", DoubleType) ::
      StructField("afr_1000g_prop", DoubleType) ::
      StructField("mar_1000g_prop", DoubleType) ::
      StructField("eas_1000g_prop", DoubleType) ::
      StructField("eur_1000g_prop", DoubleType) ::
      StructField("sas_1000g_prop", DoubleType) :: Nil)

  val finemappingSchema = StructType(
    StructField("stid", StringType) ::
      StructField("index_variant_id", StringType) ::
      StructField("tag_variant_id", StringType) ::
      StructField("log10_abf", DoubleType) ::
      StructField("posterior_prob", DoubleType) :: Nil)


  def build(vIdx: VariantIndex, conf: Configuration)(implicit ss: SparkSession): V2DIndex = {

    val studies = buildStudiesIndex(conf.variantDisease.studies)

    new V2DIndex {
      override val table: DataFrame = studies
    }
  }

  def buildStudiesIndex(path: String)(implicit ss: SparkSession): DataFrame = {
    val processTraits = udf((codes: String, labels: String) => codes.split(";")
        .zipAll(labels.split(";"),"", "")
        .filter(_._1 != "")
        .map(t => Array(t._1,t._2)))

    val studies = ss.read
      .format("csv")
      .option("header", "true")
      .option("delimiter","\t")
      .schema(studiesSchema)
      .load(path)

    val pStudies = studies
      .withColumn("trait_label", when(col("trait_efos").isNull, col("trait_reported")).otherwise(col("trait_mapped")))
      .withColumn("trait_code", when(col("trait_mapped").isNull, col("trait_reported")).otherwise(col("trait_efos")))
      .withColumn("trait_pair",
        explode(processTraits(col("trait_code"), col("trait_label"))))
      .withColumn("efo_code", col("trait_pair").getItem(0))
      .withColumn("efo_label", col("trait_pair").getItem(1))
      .drop("trait_pair")

    pStudies
  }

  /** join built gtex and vep together and generate char pos alleles columns from variant_id */
  def load(conf: Configuration)(implicit ss: SparkSession): V2DIndex = {

    logger.info("load variant to gene dataset from built one")
    val v2d = ss.read
      .format("csv")
      .option("header", "true")
      .option("delimiter","\t")
      .load(conf.variantDisease.path)

    new V2DIndex {
      /** uniform way to get the dataframe */
      override val table: DataFrame = v2d
    }
  }
}