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
  val rawColumnsWithAliases: Seq[(String, String)] = Seq(("chrom_b37","chr_id"), ("pos_b37", "position"),
    ("ref", "ref_allele"), ("alt", "alt_allele"), ("rsid", "rs_id"),
    ("vep.most_severe_consequence", "most_severe_consequence"),
    ("cadd", "cadd"), ("af", "af"))

  /** variant_id is represented as 1_123_T_C but split into columns 1 23456 T C */
  val columns: List[String] = List("chr_id", "position", "ref_allele", "alt_allele")
  /** types of the columns named in variantColumnNames */
  val columnsTypes: List[String] = List("String", "Long", "String", "String")

  val indexColumns: Seq[String] = Seq("chr_id")
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
        val vidx = new VariantIndex(idx)
        val nearests = Nearest(vidx, conf, conf.variantIndex.tssDistance,
          GeneIndex.biotypes)(ss).table.persist(StorageLevels.DISK_ONLY)


        val w = Window.partitionBy(columns.head, columns.tail:_*)

        val nearestGenesCols = columns ++ List("gene_id", "gene_id_distance")
        val nearestGenes = nearests.groupBy(columns.head, columns.tail:_*)
          .agg(min(col("d")).as("d"),
            first(col("gene_id")).as("gene_id"))
          .withColumnRenamed("d", "gene_id_distance")
          .select(nearestGenesCols.head, nearestGenesCols.tail:_*)

        val nearestPCGenesCols = columns ++ List("gene_id_prot_coding", "gene_id_prot_coding_distance")
        val nearestPCGenes = nearests.where(col("biotype") === "protein_coding")
          .groupBy(columns.head, columns.tail:_*)
          .agg(min(col("d")).as("d"),
            first(col("gene_id")).as("gene_id"))
          .withColumnRenamed("d", "gene_id_prot_coding_distance")
          .withColumnRenamed("gene_id", "gene_id_prot_coding")
          .select(nearestPCGenesCols.head, nearestPCGenesCols.tail:_*)

        val computedNearests = nearestGenes.join(nearestPCGenes, columns, "full_outer")
        computedNearests
      }

      logger.info("building variant index as specified in the configuration")
      val raw = loadRawVariantIndex(rawColumnsWithAliases)
      val nearests = computeNearests(raw)
      val jointNearest = raw.join(nearests, columns, "left_outer")

      new VariantIndex(jointNearest)
    }
  }

  /** builder object to load or build the VariantIndex */
  def builder(conf: Configuration)(implicit ss: SparkSession): Builder = new Builder(conf, ss)
}
