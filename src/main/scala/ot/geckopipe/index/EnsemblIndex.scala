package ot.geckopipe.index

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** This class represents a full table of gene with its transcripts grch37 */
abstract class EnsemblIndex extends Indexable {
  def aggByGene: DataFrame = aggBy(EnsemblIndex.indexColumns, EnsemblIndex.columns)
}

/** Companion object to build the EnsemblIndex class */
object EnsemblIndex {
  val columns: Seq[String] = Seq("gene_chr", "gene_id", "gene_start", "gene_end")
  val indexColumns: Seq[String] = Seq("gene_chr", "gene_id")

  val schema = StructType(
    StructField("gene_id", StringType) ::
      StructField("trans_id", StringType) ::
      StructField("gene_start", LongType) ::
      StructField("gene_end", LongType) ::
      StructField("trans_start", LongType) ::
      StructField("trans_end", LongType) ::
      StructField("tss", LongType) ::
      StructField("trans_size", LongType) ::
      StructField("gene_name", StringType) ::
      StructField("gene_chr", StringType) ::
      StructField("gene_type", StringType) :: Nil)

  /** load and transform lut gene transcript gene name from ensembl mart website
    *
    * columns from the tsv file from ensembl
    * - Gene stable ID
    * - Transcript stable ID
    * - Gene start (bp)
    * - Gene end (bp)
    * - Transcript start (bp)
    * - Transcript end (bp)
    * - Transcription start site (TSS)
    * - Transcript length (including UTRs and CDS)
    * - Gene name
    * - Chromosome/scaffold name
    * - Gene type
    *
    * @param from mostly from config.ensembl.geneTranscriptPairs
    * @param ss implicit sparksession
    * @return the processed dataframe
    */
  def apply(from: String)(implicit ss: SparkSession): EnsemblIndex = {
    val transcripts = ss.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .option("delimiter","\t")
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .load(from)

    new EnsemblIndex {
      override val table: DataFrame = transcripts
    }
  }
}
