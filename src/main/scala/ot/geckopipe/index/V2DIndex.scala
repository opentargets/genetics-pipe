package ot.geckopipe.index

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import ot.geckopipe.Configuration

case class V2DIndex(table: DataFrame)

object V2DIndex extends LazyLogging {
  val schema =
    StructType(
      StructField("study_id", StringType) ::
        StructField("pmid", StringType) ::
        StructField("pub_date", StringType) ::
        StructField("pub_journal", StringType) ::
        StructField("pub_title", StringType) ::
        StructField("pub_author", StringType) ::
        StructField("has_sumstats", BooleanType) ::
        StructField("trait_reported", StringType) ::
        StructField("trait_efos", ArrayType(StringType)) ::
        StructField("ancestry_initial", ArrayType(StringType)) ::
        StructField("ancestry_replication", ArrayType(StringType)) ::
        StructField("n_initial", LongType) ::
        StructField("n_replication", LongType) ::
        StructField("n_cases", LongType) ::
        StructField("trait_category", DoubleType) ::
        StructField("num_assoc_loci", LongType) ::
        StructField("lead_chrom", StringType) ::
        StructField("lead_pos", LongType) ::
        StructField("lead_ref", StringType) ::
        StructField("lead_alt", StringType) ::
        StructField("tag_chrom", StringType) ::
        StructField("tag_pos", LongType) ::
        StructField("tag_ref", StringType) ::
        StructField("tag_alt", StringType) ::
        StructField("overall_r2", DoubleType) ::
        StructField("AFR_1000G_prop", DoubleType) ::
        StructField("AMR_1000G_prop", DoubleType) ::
        StructField("EAS_1000G_prop", DoubleType) ::
        StructField("EUR_1000G_prop", DoubleType) ::
        StructField("SAS_1000G_prop", DoubleType) ::
        StructField("log10_ABF", DoubleType) ::
        StructField("posterior_prob", DoubleType) ::
        StructField("odds_ratio", DoubleType) ::
        StructField("oddsr_ci_lower", DoubleType) ::
        StructField("oddsr_ci_upper", DoubleType) ::
        StructField("direction", StringType) ::
        StructField("beta", DoubleType) ::
        StructField("beta_ci_lower", DoubleType) ::
        StructField("beta_ci_upper", DoubleType) ::
        StructField("pval_mantissa", DoubleType) ::
        StructField("pval_exponent", LongType) ::
        StructField("pval", DoubleType) :: Nil)

  def build(vIdx: VariantIndex, conf: Configuration)(implicit ss: SparkSession): V2DIndex = {
    val studies = buildStudiesIndex(conf.variantDisease.studies).cache()
    val topLoci = buildTopLociIndex(conf.variantDisease.toploci).cache()
    val ldLoci = buildLDIndex(conf.variantDisease.ld)
      .drop("ld_available", "pics_mu", "pics_postprob", "pics_95perc_credset",
        "pics_99perc_credset")
      .cache()
    val fmLoci = buildFMIndex(conf.variantDisease.finemapping).cache()

    val svPairs = topLoci.join(studies, "study_id").cache()

    logger.whenDebugEnabled {
      svPairs.show(false)
      svPairs.where(col("pval").isNull).show(false)
    }

    // ED WILL FIX THIS PROBLEMATIC ISSUE ABOUT TOPLOCI -> EXPANDED ONE
    // EACH TOPLOCI MUST BE IN THE EXPANDED TABLE
    val joinCols = Seq("study_id", "lead_chrom", "lead_pos", "lead_ref", "lead_alt",
      "tag_chrom", "tag_pos", "tag_ref", "tag_alt")
    val ldAndFm = ldLoci.join(fmLoci, joinCols, "full_outer")
    val indexExpanded = svPairs.join(ldAndFm,
      Seq("study_id", "lead_chrom", "lead_pos", "lead_ref", "lead_alt"))

    logger.whenDebugEnabled {
      indexExpanded.where(col("pval").isNull).show(false)
    }

    V2DIndex(indexExpanded.join(vIdx.table.select("chr_id", "position", "ref_allele", "alt_allele"),
      col("chr_id") === col("tag_chrom") and
        (col("position") === col("tag_pos") and
          (col("ref_allele") === col("tag_ref") and
            (col("alt_allele") === col("tag_alt")))), "inner")
      .drop("chr_id", "position", "ref_allele", "alt_allele")
    )
  }

  def buildStudiesIndex(path: String)(implicit ss: SparkSession): DataFrame =
    ss.read.parquet(path).orderBy(col("study_id").asc)

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

    ss.read.parquet(path)
      .withColumn("pval", toDouble(col("pval_mantissa"), col("pval_exponent")))
      .withColumnRenamed("chrom", "lead_chrom")
      .withColumnRenamed("pos", "lead_pos")
      .withColumnRenamed("ref", "lead_ref")
      .withColumnRenamed("alt", "lead_alt")
      // TODO tell to remove the column from source so I can remove this line too
      .drop("rsid")
      .repartitionByRange(col("lead_chrom"))
      .sortWithinPartitions(col("lead_chrom"), col("lead_pos"), col("lead_ref"), col("lead_alt"))
  }

  def buildLDIndex(path: String)(implicit ss: SparkSession): DataFrame =
    ss.read.parquet(path)
      .repartitionByRange(col("lead_chrom"))
      .sortWithinPartitions(col("lead_chrom"), col("lead_pos"), col("lead_ref"), col("lead_alt"))

  def buildFMIndex(path: String)(implicit ss: SparkSession): DataFrame =
    ss.read.parquet(path)
      .repartitionByRange(col("lead_chrom"))
      .sortWithinPartitions(col("lead_chrom"), col("lead_pos"), col("lead_ref"), col("lead_alt"))

  def buildOverlapIndex(path: String)(implicit ss: SparkSession): DataFrame = {
    val aCols = Seq("A_study_id", "A_chrom", "A_pos", "A_ref", "A_alt")
    val bCols = Seq("B_study_id", "B_chrom", "B_pos", "B_ref", "B_alt", "A_distinct",
      "AB_overlap", "B_distinct")

    val selectCols = (aCols ++ bCols).map(col)
    ss.read.parquet(path).drop("set_type")
      .select(selectCols: _*)
  }

  /** join built gtex and vep together and generate char pos alleles columns from variant_id */
  def load(conf: Configuration)(implicit ss: SparkSession): V2DIndex = {

    logger.info("load variant to gene dataset from built one")
    val v2d = ss.read
      .json(conf.variantDisease.path)

    V2DIndex(v2d)
  }
}