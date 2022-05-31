package ot.geckopipe.index

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import enumeratum._

import scala.collection.immutable

/** This class represents a full table of gene with its transcripts grch37 */
class GeneIndex(val table: DataFrame) {
  def sortByTSS: GeneIndex =
    new GeneIndex(table.sortWithinPartitions(col("chr").asc, col("tss").asc))

  def sortByID: GeneIndex =
    new GeneIndex(table.sortWithinPartitions(col("chr").asc, col(GeneIndex.idColumn).asc))
}

/** Companion object to build the GeneIndex class */
object GeneIndex {

  sealed abstract class BioTypes(override val entryName: String, val set: Set[String])
      extends EnumEntry

  object BioTypes extends Enum[BioTypes] {

    /*
     `findValues` is a protected method that invokes a macro to find all `Greeting` object declarations inside an `Enum`

     You use it to implement the `val values` member
     */
    val values: immutable.IndexedSeq[BioTypes] = findValues

    case object ProteinCoding extends BioTypes("protein_coding", Set("protein_coding"))

    case object ApprovedBioTypes extends BioTypes("filtered_biotypes", approvedBioTypes)

    val approvedBioTypes = Set(
      "3prime_overlapping_ncRNA",
      "antisense",
      "bidirectional_promoter_lncRNA",
      "IG_C_gene",
      "IG_D_gene",
      "IG_J_gene",
      "IG_V_gene",
      "lincRNA",
      "macro_lncRNA",
      "non_coding",
      "protein_coding",
      "sense_intronic",
      "sense_overlapping"
    )
  }

  val chromosomes: Set[String] = Set("MT")

  /** A subset of all possible gene columns that can be included
    *
    * { "gene_id": "ENSG00000223972", "gene_name": "DDX11L1", "description": "DEAD/H
    * (Asp-Glu-Ala-Asp/His) box helicase 11 like 1 [Source:HGNC Symbol;Acc:37102]", "biotype":
    * "pseudogene", "chr": "1", "tss": 11869, "start": 11869, "end": 14412, "fwdstrand": 1, "exons":
    * "[11869,12227,12613,12721,13221,14409]" }
    */
  val columns: Seq[String] = Seq("chr", "gene_id", "tss", "start", "end", "biotype")
  val indexColumns: Seq[String] = Seq("chr")
  val idColumn: String = "gene_id"

  /** load and transform lut gene from ensembl
    *
    * @param from
    *   mostly from config.ensembl.lut
    * @param ss
    *   implicit sparksession
    * @return
    *   the processed dataframe
    */
  def apply(from: String, bioTypes: BioTypes = BioTypes.ApprovedBioTypes)(implicit
      ss: SparkSession
  ): GeneIndex = {
    val indexCols = indexColumns.map(c => col(c).asc)
    val genes = ss.read
      .json(from)
      .where(
        (col("biotype") isInCollection bioTypes.set) and
          !(col("chr") isInCollection chromosomes)
      )
      .repartitionByRange(indexCols: _*)

    new GeneIndex(genes)
  }
}
