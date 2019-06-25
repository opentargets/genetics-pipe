package ot.geckopipe.domain

case class Variant2(chr_id: String,
                    position: Long,
                    ref_allele: String,
                    alt_allele: String,
                    rs_id: Option[String] = None,
                    most_severe_consequence: Option[String] = None,
                    cadd: Cadd,
                    af: Gnomad,
                    gene_id_any_distance: Option[Long] = None,
                    gene_id_any: Option[String] = None,
                    gene_id_prot_coding_distance: Option[Long] = None,
                    gene_id_prot_coding: Option[String] = None)
