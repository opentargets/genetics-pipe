package ot.geckopipe.domain

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.geckopipe.Configuration

import scala.util.Random

case class Manhattan(study: String,
                     chrom: String,
                     pos: Long,
                     ref: String,
                     alt: String,
                     pval: Double,
                     pval_mantissa: Double,
                     pval_exponent: Long,
                     odds: Option[Double],
                     oddsL: Option[Double],
                     oddsU: Option[Double],
                     direction: Option[String],
                     beta: Option[Double],
                     betaL: Option[Double],
                     betaU: Option[Double],
                     credibleSetSize: Long,
                     ldSetSize: Long,
                     uniq_variants: Long,
                     top10_genes_raw_ids: List[String],
                     top10_genes_raw_score: List[Double],
                     top10_genes_coloc_ids: List[String],
                     top10_genes_coloc_score: List[Double],
                     top10_genes_l2g_ids: List[String],
                     top10_genes_l2g_score: List[Double])

object Manhattan {
  private def computeTopNGenes(uniqCols: List[String],
                               geneIdCol: String,
                               geneScoreCol: String,
                               outputCol: String,
                               n: Int)(df: DataFrame): DataFrame = {

    val outExpr = uniqCols ++ List(
      s"${outputCol}.id as ${outputCol}_ids",
      s"${outputCol}.score as ${outputCol}_score",
    )

    val w = Window.partitionBy(uniqCols.map(col): _*)
    val tmpC = Random.alphanumeric.take(6).mkString
    df.withColumn(tmpC, dense_rank().over(w.orderBy(col(geneScoreCol).desc)))
      .filter(col(tmpC) <= n)
      .withColumn(outputCol,
                  collect_list(struct(col(geneScoreCol).as("score"), col(geneIdCol).as("id")))
                    .over(w.orderBy(col(tmpC).asc)))
      .dropDuplicates(uniqCols)
      .selectExpr(outExpr: _*)
  }

  def makeL2G(cols: List[String])(df: DataFrame): DataFrame = {
    df.withColumnRenamed("study_id", "study")
      .filter(col("y_proba_full_model") >= 0.5)
      .transform(computeTopNGenes(cols, "gene_id", "y_proba_full_model", "top10_genes_l2g", 10))
  }

  def makeD2V2G(cols: List[String])(df: DataFrame): DataFrame = {
    df.withColumnRenamed("study_id", "study")
      .withColumnRenamed("lead_chrom", "chrom")
      .withColumnRenamed("lead_pos", "pos")
      .withColumnRenamed("lead_ref", "ref")
      .withColumnRenamed("lead_alt", "alt")
      .transform(computeTopNGenes(cols, "gene_id", "overall_score", "top10_genes_raw", 10))
  }

  def makeD2VColoc(cols: List[String])(df: DataFrame): DataFrame = {
    df.withColumnRenamed("left_study", "study")
      .withColumnRenamed("left_chrom", "chrom")
      .withColumnRenamed("left_pos", "pos")
      .withColumnRenamed("left_ref", "ref")
      .withColumnRenamed("left_alt", "alt")
      .filter((round(col("coloc_h4"), 2) >= 0.95) and
        (col("coloc_log2_h4_h3") >= log2(lit(5D))) and
        !(col("right_type") === "gwas"))
      .transform(computeTopNGenes(cols, "right_gene_id", "coloc_h4", "top10_genes_coloc", 10))
  }

  def makeD2V(cols: List[String])(df: DataFrame): DataFrame = {
    val tagVariantCols = List("tag_chrom", "tag_pos", "tag_ref", "tag_alt").map(col)
    val randomC = Random.alphanumeric.take(6).mkString("", "_", "")
    df.withColumnRenamed("study_id", "study")
      .withColumnRenamed("lead_chrom", "chrom")
      .withColumnRenamed("lead_pos", "pos")
      .withColumnRenamed("lead_ref", "ref")
      .withColumnRenamed("lead_alt", "alt")
      .withColumn(randomC, concat(tagVariantCols: _*))
      .groupBy(cols.map(col): _*)
      .agg(
        first(col("pval")).as("pval"),
        first(col("pval_mantissa")).as("pval_mantissa"),
        first(col("pval_exponent")).as("pval_exponent"),
        first(col("odds_ratio")).as("odds"),
        first(col("oddsr_ci_lower")).as("oddsL"),
        first(col("oddsr_ci_upper")).as("oddsU"),
        first(col("direction")).as("direction"),
        first(col("beta")).as("beta"),
        first(col("beta_ci_lower")).as("betaL"),
        first(col("beta_ci_upper")).as("betaU"),
        countDistinct(when(col("posterior_prob") > 0D, randomC).otherwise(null))
          .as("credibleSetSize"),
        countDistinct(when(col("overall_r2") > 0D, randomC).otherwise(null))
          .as("ldSetSize"),
        countDistinct(col(randomC)).as("uniq_variants")
      )
  }

  def apply(configuration: Configuration)(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._
    val conf = configuration.manhattan

    val cols = List("study", "chrom", "pos", "ref", "alt")
    val l2g = sparkSession.read
      .format("parquet")
      .load(conf.locusGene)
      .transform(makeL2G(cols))

    val d2v2g = sparkSession.read
      .format(configuration.format)
      .load(conf.diseaseVariantGeneScored)
      .transform(makeD2V2G(cols))

    val coloc = sparkSession.read
      .format(configuration.format)
      .load(conf.variantDiseaseColoc)
      .transform(makeD2VColoc(cols))

    val v2d = sparkSession.read
      .format(configuration.format)
      .load(conf.variantDisease)
      .transform(makeD2V(cols))

    v2d
      .join(d2v2g, cols, "left_outer")
      .join(coloc, cols, "left_outer")
      .join(l2g, cols, "left_outer")
      .as[Manhattan]
      .map(identity)
  }
}
