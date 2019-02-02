package ot.geckopipe.index

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import ot.geckopipe.Configuration
import ot.geckopipe.functions._

abstract class V2DIndex extends Indexable

object V2DIndex extends LazyLogging  {
  def build(vIdx: VariantIndex, conf: Configuration)(implicit ss: SparkSession): V2DIndex = {
    val studies = buildStudiesIndex(conf.variantDisease.studies).cache()
    val topLoci = buildTopLociIndex(conf.variantDisease.toploci).cache()
    val ldLoci = buildLDIndex(conf.variantDisease.ld).cache()
    val fmLoci = buildFMIndex(conf.variantDisease.finemapping).cache()
    val overlapStudies = buildOverlapIndex(conf.variantDisease.overlapping).cache()

    val indexVariants = studies.join(topLoci, "study_id")
      .orderBy(col("lead_chrom"), col("lead_pos"), col("lead_ref"), col("lead_alt")).cache()

    logger.whenDebugEnabled {
      indexVariants.show(false)
      indexVariants.where(col("pval").isNull).show(false)
    }

    // ED WILL FIX THIS PROBLEMATIC ISSUE ABOUT TOPLOCI -> EXPANDED ONE
    // EACH TOPLOCI MUST BE IN THE EXPANDED TABLE
    val joinCols = Seq("study_id", "lead_chrom", "lead_pos", "lead_ref", "lead_alt",
      "tag_chrom", "tag_pos", "tag_ref", "tag_alt")
    val ldAndFm = ldLoci.join(fmLoci, joinCols, "full_outer")
    val indexExpanded = indexVariants.join(ldAndFm,
      Seq("study_id", "lead_chrom", "lead_pos", "lead_ref", "lead_alt"))
//      .withColumn("variant_id", when(col("tag_variant_id").isNull, col("index_variant_id"))
//        .otherwise(col("tag_variant_id")))
//      .drop("tag_variant_id")

    logger.whenDebugEnabled {
      indexExpanded.where(col("pval").isNull).show(false)
    }

//    val ldAndFmEnriched = splitVariantID(indexExpanded).get
//      .repartitionByRange(col("chr_id").asc, col("position").asc)
//      .join(vIdx.table.select("chr_id", "position", "variant_id", "rs_id"), Seq("chr_id", "position", "variant_id"), "left_outer")

    new V2DIndex {
      override val table: DataFrame = indexExpanded
    }
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

    //message schema {
    //  optional binary A_study_id (STRING);
    //  optional binary A_chrom (STRING);
    //  optional int64 A_pos;
    //  optional binary A_ref (STRING);
    //  optional binary A_alt (STRING);
    //  optional binary B_study_id (STRING);
    //  optional binary B_chrom (STRING);
    //  optional int64 B_pos;
    //  optional binary B_ref (STRING);
    //  optional binary B_alt (STRING);
    //  optional binary set_type (STRING);
    //  optional int64 A_distinct;
    //  optional int64 AB_overlap;
    //  optional int64 B_distinct;
    //}
    aggregation.orderBy(col("A_study_id"), col("A_chrom"), col("A_pos"),
      col("A_ref"), col("A_alt"))

  }

  /** join built gtex and vep together and generate char pos alleles columns from variant_id */
  def load(conf: Configuration)(implicit ss: SparkSession): V2DIndex = {

    logger.info("load variant to gene dataset from built one")
    val v2d = ss.read
      .parquet(conf.variantDisease.path)

    new V2DIndex {
      /** uniform way to get the dataframe */
      override val table: DataFrame = v2d
    }
  }
}