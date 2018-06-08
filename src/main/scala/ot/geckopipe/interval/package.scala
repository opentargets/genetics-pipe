package ot.geckopipe

package object interval {
  /** interval columns
    *
    * 1 23456 123 ENSG0000002 pchic unknown promoter [score1, score2, ...]
    */
  val intervalColumnNames: List[String] = List("chr_id", "position_start", "position_end",
    "gene_id", "source_id", "tissue_id", "feature", "value")
}
