package ot.geckopipe.domain

case class Variant(chr_id: String,
                   position: Int,
                   chr_id_b37: String,
                   position_b37: Int,
                   ref_allele: String,
                   alt_allele: String,
                   rs_id: String,
                   most_severe_consequence: String,
                   cadd: String,
                   af: String,
                   gene_id_any_distance: Long,
                   gene_id_any: String,
                   gene_id_prot_coding_distance: Long,
                   gene_id_prot_coding: String)
