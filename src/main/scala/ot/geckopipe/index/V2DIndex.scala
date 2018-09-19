package ot.geckopipe.index

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import ot.geckopipe.Configuration
import ot.geckopipe.functions._

abstract class V2DIndex extends Indexable {
  def leanTable : DataFrame = selectBy(V2DIndex.indexColumns ++ V2DIndex.columns)
}

object V2DIndex extends LazyLogging  {
  val columns: Seq[String] = Seq("stid", "pmid", "index_chr_id", "index_position",
    "index_ref_allele", "index_alt_allele", "index_rs_id", "n_initial", "n_replication", "tag_variant_id",
    "r2", "afr_1000g_prop", "amr_1000g_prop", "eas_1000g_prop", "eur_1000g_prop",
    "sas_1000g_prop", "log10_abf", "posterior_prob")
  val indexColumns: Seq[String] = Seq("stid", "index_variant_id")

  val fullSchema = StructType(
    StructField("chr_id", StringType) ::
    StructField("position", LongType) ::
    StructField("ref_allele", StringType) ::
    StructField("alt_allele", StringType) ::
    StructField("stid", StringType, false) ::
    StructField("index_variant_id", StringType) ::
    StructField("r2", DoubleType) ::
    StructField("afr_1000g_prop", DoubleType) ::
    StructField("amr_1000g_prop", DoubleType) ::
    StructField("eas_1000g_prop", DoubleType) ::
    StructField("eur_1000g_prop", DoubleType) ::
    StructField("sas_1000g_prop", DoubleType) ::
    StructField("log10_abf", DoubleType) ::
    StructField("posterior_prob", DoubleType) ::
    StructField("pmid", StringType) ::
    StructField("pub_date", StringType) ::
    StructField("pub_journal", StringType) ::
    StructField("pub_title", StringType) ::
    StructField("pub_author", StringType) ::
    StructField("trait_reported", StringType, false) ::
    StructField("trait_efos", ArrayType(StringType)) ::
    StructField("trait_code", StringType, false) ::
    StructField("ancestry_initial", StringType) ::
    StructField("ancestry_replication", StringType) ::
    StructField("n_initial", LongType) ::
    StructField("n_replication", LongType) ::
    StructField("n_cases", LongType) ::
    StructField("trait_category", StringType) ::
    StructField("pval", DoubleType, false) ::
    StructField("index_variant_rsid", StringType) ::
    StructField("index_chr_id", StringType) ::
    StructField("index_position", LongType) ::
    StructField("index_ref_allele", StringType) ::
    StructField("index_alt_allele", StringType) ::
    StructField("variant_id", StringType) ::
    StructField("rs_id", StringType) :: Nil)

  val studiesSchema = StructType(
    StructField("stid", StringType, false) ::
      StructField("pmid", StringType) ::
      StructField("pub_date", StringType) ::
      StructField("pub_journal", StringType) ::
      StructField("pub_title", StringType) ::
      StructField("pub_author", StringType) ::
      StructField("trait_reported", StringType, false) ::
      StructField("trait_efos", StringType) ::
      StructField("trait_code", StringType, false) ::
      StructField("ancestry_initial", StringType) ::
      StructField("ancestry_replication", StringType) ::
      StructField("n_initial", LongType) ::
      StructField("n_replication", LongType) ::
      StructField("n_cases", LongType) ::
      StructField("trait_category", StringType) :: Nil)

  val topLociSchema = StructType(
    StructField("stid", StringType, false) ::
      StructField("variant_id", StringType, false) ::
      StructField("rs_id", StringType) ::
      StructField("pval_mantissa", DoubleType, false) ::
      StructField("pval_exponent", DoubleType, false) :: Nil)

  val ldSchema = StructType(
    StructField("stid", StringType, false) ::
      StructField("index_variant_id", StringType, false) ::
      StructField("tag_variant_id", StringType, false) ::
      StructField("r2", DoubleType) ::
      StructField("afr_1000g_prop", DoubleType) ::
      StructField("amr_1000g_prop", DoubleType) ::
      StructField("eas_1000g_prop", DoubleType) ::
      StructField("eur_1000g_prop", DoubleType) ::
      StructField("sas_1000g_prop", DoubleType) :: Nil)

  val finemappingSchema = StructType(
    StructField("stid", StringType, false) ::
      StructField("index_variant_id", StringType, false) ::
      StructField("tag_variant_id", StringType, false) ::
      StructField("log10_abf", DoubleType) ::
      StructField("posterior_prob", DoubleType) :: Nil)

  def build(vIdx: VariantIndex, conf: Configuration)(implicit ss: SparkSession): V2DIndex = {
    val studies = buildStudiesIndex(conf.variantDisease.studies)
    val topLoci = buildTopLociIndex(conf.variantDisease.toploci)
    val ldLoci = buildLDIndex(conf.variantDisease.ld)
    val fmLoci = buildFMIndex(conf.variantDisease.finemapping)

    val indexVariants = studies.join(topLoci, Seq("stid"))
    val ldAndFm = ldLoci.join(fmLoci, Seq("stid", "index_variant_id", "tag_variant_id"), "full_outer")
    val indexExpanded = ldAndFm.join(indexVariants, Seq("stid", "index_variant_id"), "full_outer")
      .withColumn("variant_id", when(col("tag_variant_id").isNull, col("index_variant_id"))
        .otherwise(col("tag_variant_id")))
      .drop("tag_variant_id")

    val ldAndFmEnriched = splitVariantID(indexExpanded).get
      .repartitionByRange(col("chr_id").asc, col("position").asc)
      .join(vIdx.table.select("chr_id", "position", "variant_id", "rs_id"), Seq("chr_id", "position", "variant_id"), "left_outer")

    new V2DIndex {
      override val table: DataFrame = ldAndFmEnriched
    }
  }

  def buildStudiesIndex(path: String)(implicit ss: SparkSession): DataFrame = {
    val studies = loadFromCSV(path, studiesSchema)
    val removeSuffix = udf((col: String) => col.split("\\.").head)

    val pStudies = studies
      .withColumn("trait_efos", when(col("trait_efos").isNotNull,
        split(col("trait_efos"),";")))
//      .withColumn("n_cases", when(col("n_cases").isNotNull,col("n_cases").cast(LongType)))
//      .withColumn("n_initial", when(col("n_initial").isNotNull,col("n_initial").cast(LongType)))
//      .withColumn("n_replication", when(col("n_replication").isNotNull,col("n_replication").cast(LongType)))

    pStudies
  }

  def buildTopLociIndex(path: String)(implicit ss: SparkSession): DataFrame = {
    val toDouble = udf((mantissa: Double, exponent: Double) => {
      val result = mantissa * Math.pow(10, exponent)
      result match {
        case Double.PositiveInfinity => Double.MaxValue
        case Double.NegativeInfinity => Double.MinValue
        case 0.0 => Double.MinPositiveValue
        case -0.0 => -Double.MinPositiveValue
        case _ => result
      }
    })

    val loci = loadFromCSV(path, topLociSchema)

    val fLoci = loci
      .withColumn("pval", toDouble(col("pval_mantissa"), col("pval_exponent")))
      .withColumn("_splitAndZip", explode(splitAndZip(col("variant_id"), col("rs_id"))))
      .withColumn("index_variant_id", col("_splitAndZip").getItem(0))
      .withColumn("index_variant_rsid", col("_splitAndZip").getItem(1))

    splitVariantID(fLoci, "index_variant_id", "index_").get
      .drop("pval_mantissa", "pval_exponent", "variant_id", "rs_id", "_splitAndZip")
  }

  def buildLDIndex(path: String)(implicit ss: SparkSession): DataFrame = {
    val ld = loadFromCSV(path, ldSchema)

    ld
  }


  def buildFMIndex(path: String)(implicit ss: SparkSession): DataFrame = {
    val fm = loadFromCSV(path, finemappingSchema)

    fm
  }

  /** join built gtex and vep together and generate char pos alleles columns from variant_id */
  def load(conf: Configuration)(implicit ss: SparkSession): V2DIndex = {

    logger.info("load variant to gene dataset from built one")
    val v2d = ss.read
      .schema(fullSchema)
      .json(conf.variantDisease.path)

    new V2DIndex {
      /** uniform way to get the dataframe */
      override val table: DataFrame = v2d
    }
  }
}