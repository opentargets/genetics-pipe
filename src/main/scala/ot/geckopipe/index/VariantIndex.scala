package ot.geckopipe.index

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import ot.geckopipe.functions.loadFromParquet
import ot.geckopipe.{Configuration, Nearest}

/** represents a cached table of variants with all variant columns
  *
  * columns as chr_id, position, ref_allele, alt_allele, variant_id, rs_id. Also
  * this table is persisted and sorted by (chr_id, position) by default
  */
class VariantIndex(val table: DataFrame) {
  def schema: StructType = table.schema
  def flatten: VariantIndex =
    new VariantIndex(table.select(col("*"), col("cadd.*"), col("af.*")).drop("cadd", "af"))
}

/** The companion object helps to build VariantIndex from Configuration and SparkSession */
object VariantIndex {
  case class VIRow(chr_id: String, position: Long, ref_allele: String, alt_allele: String,
                   d: Long, gene_id: String)

  val rawColumnsWithAliases: Seq[(String, String)] = Seq(("chrom_b37","chr_id"), ("pos_b37", "position"),
    ("ref", "ref_allele"), ("alt", "alt_allele"), ("rsid", "rs_id"),
    ("vep.most_severe_consequence", "most_severe_consequence"),
    ("cadd", "cadd"), ("af", "af"))

  /** variant_id is represented as 1_123_T_C but split into columns 1 23456 T C */
  val columns: List[String] = List("chr_id", "position", "ref_allele", "alt_allele")
  /** types of the columns named in variantColumnNames */
  val columnsTypes: List[String] = List("String", "Long", "String", "String")

  val indexColumns: Seq[String] = Seq("chr_id", "position")
  val sortColumns: Seq[String] = Seq("chr_id", "position")

  /** this class build based on the Configuration it creates a VariantIndex */
  class Builder (val conf: Configuration, val ss: SparkSession) extends LazyLogging {
    def load: VariantIndex = {
      logger.info("loading variant index as specified in the configuration")
      val vIdx = ss.read.parquet(conf.variantIndex.path).persist(StorageLevels.DISK_ONLY)

      new VariantIndex(vIdx)
    }

    def loadRawVariantIndex(columnsWithAliases: Seq[(String, String)]): DataFrame = {
      val indexCols = indexColumns.map(c => col(c).asc)
      val sortCols = sortColumns.map(c => col(c).asc)
      val inputCols = columnsWithAliases.map(s => col(s._1).alias(s._2))
      val raw = loadFromParquet(conf.variantIndex.raw)(ss)

      raw.select(inputCols:_*)
        .repartitionByRange(indexCols:_*)
        .sortWithinPartitions(sortCols:_*)
    }

    def build: VariantIndex = {
      def computeNearests(idx: DataFrame): DataFrame = {
        import ss.implicits._

        val vidx = new VariantIndex(idx)

        val nearests = Nearest(vidx, conf,
          conf.variantIndex.tssDistance,
          GeneIndex.biotypes)(ss).table

        val nearestGenes = nearests
          .as[VIRow]
          .groupByKey(r => (r.chr_id, r.position, r.ref_allele, r.alt_allele))
          .reduceGroups((r1, r2) => if (r1.d < r2.d) r1 else r2)
          .map(_._2)
          .toDF.withColumnRenamed("d", "gene_id__any_distance")
          .withColumnRenamed("gene_id", "gene_id_any")

        val nearestsPC = Nearest(vidx, conf,
          conf.variantIndex.tssDistance,
          Set("protein_coding"))(ss).table

        val nearestPCGenes = nearestsPC
          .as[VIRow]
          .groupByKey(r => (r.chr_id, r.position, r.ref_allele, r.alt_allele))
          .reduceGroups((r1, r2) => if (r1.d < r2.d) r1 else r2)
          .map(_._2)
          .toDF
          .withColumnRenamed("d", "gene_id_prot_coding_distance")
          .withColumnRenamed("gene_id", "gene_id_prot_coding")

        val computedNearests = nearestGenes.join(nearestPCGenes, columns, "full_outer")
        computedNearests
      }

      logger.info("building variant index as specified in the configuration")
      val raw = loadRawVariantIndex(rawColumnsWithAliases).persist(StorageLevels.DISK_ONLY)
      val nearests = computeNearests(raw).persist(StorageLevels.DISK_ONLY)
      val jointNearest = raw.join(nearests, columns, "left_outer")

      new VariantIndex(jointNearest)
    }
  }

  /** builder object to load or build the VariantIndex */
  def builder(conf: Configuration)(implicit ss: SparkSession): Builder = new Builder(conf, ss)
}
