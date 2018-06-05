package ot

package object geckopipe {
  /** all data sources to incorporate needs to meet this format at the end
    *
    * One example of the shape of the data could be
    * "1_123_T_C ENSG0000001 gtex uberon_0001 1
    */
  val v2gColumnNames: List[String] = List("variant_id", "gene_id", "source_id", "tissue_id",
    "feature", "value")

  /** interval columns
    *
    * 1 23456 123 ENSG0000002 pchic unknown promoter [score1, score2, ...]
    */
  val intervalColumnNames: List[String] = List("chr_id", "position_start", "position_end",
    "gene_id", "source_id", "tissue_id", "feature", "value")

  /** variant_id is represented as 1_123_T_C but splitted into columns
    *
    * 1 23456 T C
    */
  val variantColumnNames: List[String] = List("chr_id", "position", "ref_allele", "alt_allele")
}
