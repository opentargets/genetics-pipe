package ot.geckopipe.index

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import ot.geckopipe.functions.{loadFromCSV, splitVariantID, loadFromParquet}
import ot.geckopipe.Configuration

import scala.util._

/** represents a cached table of variants with all variant columns
  *
  * columns as chr_id, position, ref_allele, alt_allele, variant_id, rs_id. Also
  * this table is persisted and sorted by (chr_id, position) by default
  */
abstract class VariantIndex extends Indexable {
  def schema: StructType = table.schema
  def aggByVariant: DataFrame = aggBy(VariantIndex.indexColumns, VariantIndex.columns)
  def flatten: DataFrame = table.select(col("*"), col("cadd.*"), col("af.*")).drop("cadd", "af")
}

/** The companion object helps to build VariantIndex from Configuration and SparkSession */
object VariantIndex {
  val rawColumnsWithAliases: Seq[(String, String)] = Seq(("chrom_b37","chr_id"), ("pos_b37", "position"),
    ("ref", "ref_allele"), ("alt", "alt_allele"), ("rsid", "rs_id"),
    ("vep.most_severe_consequence", "most_severe_consequence"),
    ("cadd", "cadd"), ("af", "af"))
  val columns: Seq[String] = Seq("chr_id", "position", "ref_allele", "alt_allele", "variant_id", "rs_id")
  val indexColumns: Seq[String] = Seq("chr_id", "position")

  /** variant_id is represented as 1_123_T_C but split into columns 1 23456 T C */
  val variantColumnNames: List[String] = List("chr_id", "position", "ref_allele", "alt_allele")

  /** types of the columns named in variantColumnNames */
  val variantColumnTypes: List[String] = List("String", "long", "string", "string")

  val nearestGenesSchema = StructType(
    StructField("varid", StringType, nullable = false) ::
      StructField("gene_id_prot_coding", StringType) ::
      StructField("gene_id_prot_coding_distance", LongType) ::
      StructField("gene_id", StringType) ::
      StructField("gene_id_distance", LongType) :: Nil)

  /** this class build based on the Configuration it creates a VariantIndex */
  class Builder (val conf: Configuration, val ss: SparkSession) extends LazyLogging {
    def load: VariantIndex = {
      logger.info("loading variant index as specified in the configuration")
      // load from configuration
      val vIdx = ss.read
        .format("parquet")
        .load(conf.variantIndex.path)
        .persist()

      new VariantIndex {
        override val table: DataFrame = vIdx
      }
    }

    def loadNearestGenes: Try[DataFrame] = {
      splitVariantID(loadFromCSV(conf.variantIndex.nearestGenes, nearestGenesSchema)(ss),
        variantColName = "varid").map(df => {
        df.drop("varid", "gene_id_prot_coding_distance", "gene_id_distance")
          .repartitionByRange(col("chr_id").asc, col("position").asc)
          .sortWithinPartitions(col("chr_id").asc, col("position").asc)
      })
    }

    def loadRawVariantIndex(columnsWithAliases: Seq[(String, String)]): DataFrame = {
      val inputCols = columnsWithAliases.map(s => col(s._1).alias(s._2))
      val raw = loadFromParquet(conf.variantIndex.raw)(ss)
      raw.select(inputCols:_*)
        .repartitionByRange(col("chr_id").asc, col("position").asc)
        .sortWithinPartitions(col("chr_id").asc, col("position").asc)
    }

    def build: VariantIndex = {
      logger.info("building variant index as specified in the configuration")
      val savePath = conf.variantIndex.path

      val raw = loadRawVariantIndex(rawColumnsWithAliases)

      val rawWithNGenes = loadNearestGenes match {
        case Success(df) => raw.join(df, variantColumnNames, "left_outer")
        case Failure(ex) =>
          logger.error(ex.toString)
          raw
      }

      rawWithNGenes.write.parquet(savePath)

      new VariantIndex {
        override val table: DataFrame = rawWithNGenes
      }
    }
  }

  /** builder object to load or build the VariantIndex */
  def builder(conf: Configuration)(implicit ss: SparkSession): Builder = new Builder(conf, ss)
}
