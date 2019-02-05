package ot.geckopipe.index

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

/** This class represents a full table of gene with its transcripts grch37 */
class GeneIndex(val table: DataFrame) {
  def filterBiotypes(biotypes: Set[String]): GeneIndex = new GeneIndex(
    table.where(col("biotype") isInCollection biotypes))

  def sortByTSS: GeneIndex = new GeneIndex(
    table.sortWithinPartitions(col("chr").asc, col("tss").asc))

  def sortByID: GeneIndex = new GeneIndex(
    table.sortWithinPartitions(col("chr").asc, col("gene_id").asc))
}

/** Companion object to build the GeneIndex class */
object GeneIndex {
  val biotypes: Set[String] = Set("3prime_overlapping_ncrna", "antisense", "IG_C_gene",
    "IG_C_pseudogene", "IG_D_gene", "IG_J_gene", "IG_J_pseudogene", "IG_V_gene",
    "IG_V_pseudogene", "lincRNA", "miRNA", "misc_RNA", "Mt_rRNA", "Mt_tRNA",
    "polymorphic_pseudogene", "processed_transcript", "protein_coding", "pseudogene",
    "rRNA", "sense_intronic", "sense_overlapping", "snoRNA", "snRNA", "TR_C_gene",
    "TR_D_gene", "TR_J_gene", "TR_J_pseudogene", "TR_V_gene", "TR_V_pseudogene")

  /** A subset of all possible gene columns that can be included
    *
    * {
    * "gene_id": "ENSG00000223972",
    * "gene_name": "DDX11L1",
    * "description": "DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1 [Source:HGNC Symbol;Acc:37102]",
    * "biotype": "pseudogene",
    * "chr": "1",
    * "tss": 11869,
    * "start": 11869,
    * "end": 14412,
    * "fwdstrand": 1,
    * "exons": "[11869,12227,12613,12721,13221,14409]"
    * }
    */
  val columns: Seq[String] = Seq("chr", "gene_id", "tss", "start", "end", "biotype")
  val indexColumns: Seq[String] = Seq("chr", "gene_id")

  /** load and transform lut gene from ensembl
    *
    * @param from mostly from config.ensembl.lut
    * @param ss implicit sparksession
    * @return the processed dataframe
    */
  def apply(from: String)(implicit ss: SparkSession): GeneIndex = {
    val genes = ss.read.json(from)
      .repartitionByRange(col("chr").asc)
      .where(col("biotype") isInCollection biotypes)

    new GeneIndex(genes)
  }
}




