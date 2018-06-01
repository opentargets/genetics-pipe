package ot

package object geckopipe {
  /** all data sources to incorporate needs to meet this format at the end
    *
    * One example of the shape of the data could be
    * "1_123_T_C ENSG0000001 gtex uberon_0001 1
    */
  val columnNames: List[String] = List("variant_id", "gene_id", "source_id", "tissue_id",
    "feature", "value")
}
