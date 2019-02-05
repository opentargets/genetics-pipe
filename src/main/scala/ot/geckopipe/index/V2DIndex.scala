package ot.geckopipe.index

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import ot.geckopipe.Configuration

case class V2DIndex(table: DataFrame)

object V2DIndex extends LazyLogging  {
  def build(vIdx: VariantIndex, conf: Configuration)(implicit ss: SparkSession): V2DIndex = {
    val studies = buildStudiesIndex(conf.variantDisease.studies).cache()
    val topLoci = buildTopLociIndex(conf.variantDisease.toploci).cache()
    val ldLoci = buildLDIndex(conf.variantDisease.ld).cache()
    val fmLoci = buildFMIndex(conf.variantDisease.finemapping).cache()
    val overlapStudies = buildOverlapIndex(conf.variantDisease.overlapping).cache()

    val svPairs = studies.join(topLoci, "study_id")
      .orderBy(col("lead_chrom"), col("lead_pos"), col("lead_ref"), col("lead_alt")).cache()

    logger.whenDebugEnabled {
      svPairs.show(false)
      svPairs.where(col("pval").isNull).show(false)
    }

    val svPairsOverlap = svPairs.join(overlapStudies, (col("study_id") === col("A_study_id")) and
      (col("A_chrom") === col("lead_chrom")) and
      (col("A_pos") === col("lead_pos")) and
      (col("A_ref") === col("lead_ref")) and
      (col("A_alt") === col("lead_alt")),"left_outer")
      .drop("A_study_id", "A_chrom", "A_pos", "A_ref", "A_alt")

    svPairsOverlap.show(false)

    // ED WILL FIX THIS PROBLEMATIC ISSUE ABOUT TOPLOCI -> EXPANDED ONE
    // EACH TOPLOCI MUST BE IN THE EXPANDED TABLE
    val joinCols = Seq("study_id", "lead_chrom", "lead_pos", "lead_ref", "lead_alt",
      "tag_chrom", "tag_pos", "tag_ref", "tag_alt")
    val ldAndFm = ldLoci.join(fmLoci, joinCols, "full_outer")
    val indexExpanded = svPairsOverlap.join(ldAndFm,
      Seq("study_id", "lead_chrom", "lead_pos", "lead_ref", "lead_alt"))
//      .withColumn("variant_id", when(col("tag_variant_id").isNull, col("index_variant_id"))
//        .otherwise(col("tag_variant_id")))
//      .drop("tag_variant_id")

    logger.whenDebugEnabled {
      indexExpanded.where(col("pval").isNull).show(false)
    }

    V2DIndex(indexExpanded)
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

    ss.read.parquet(path).orderBy(col("study_id").asc)
      .withColumn("pval", toDouble(col("pval_mantissa"), col("pval_exponent")))
      .withColumnRenamed("chrom", "lead_chrom")
      .withColumnRenamed("pos", "lead_pos")
      .withColumnRenamed("ref", "lead_ref")
      .withColumnRenamed("alt", "lead_alt")
      .orderBy(col("study_id").asc)
  }

  def buildLDIndex(path: String)(implicit ss: SparkSession): DataFrame =
    ss.read.parquet(path)
      .orderBy(col("lead_chrom"), col("lead_pos"), col("lead_ref"), col("lead_alt"))

  def buildFMIndex(path: String)(implicit ss: SparkSession): DataFrame =
    ss.read.parquet(path)
      .orderBy(col("lead_chrom"), col("lead_pos"), col("lead_ref"), col("lead_alt"))

  def buildOverlapIndex(path: String)(implicit ss: SparkSession): DataFrame = {
    val groupCols = Seq("A_study_id", "A_chrom", "A_pos", "A_ref", "A_alt")
    val aggCols = Seq("B_study_id", "B_chrom", "B_pos", "B_ref", "B_alt", "A_distinct",
    "AB_overlap", "B_distinct").map(c => collect_list(c).as(c))
    val aggregation = ss.read.parquet(path).drop("set_type")
      .groupBy(groupCols.head, groupCols.tail:_*)
      .agg(aggCols.head, aggCols.tail:_*)

    aggregation.orderBy(col("A_study_id"), col("A_chrom"), col("A_pos"),
      col("A_ref"), col("A_alt"))
  }

  /** join built gtex and vep together and generate char pos alleles columns from variant_id */
  def load(conf: Configuration)(implicit ss: SparkSession): V2DIndex = {

    logger.info("load variant to gene dataset from built one")
    val v2d = ss.read
      .parquet(conf.variantDisease.path)

    V2DIndex(v2d)
  }
}