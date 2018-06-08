package ot.geckopipe

package object positional {
  /** variant_id is represented as 1_123_T_C but splitted into columns 1 23456 T C */
  val variantColumnNames: List[String] = List("chr_id", "position", "ref_allele", "alt_allele")
  /** types of the columns named in variantColumnNames */
  val variantColumnTypes: List[String] = List("String", "long", "string", "string")
}
