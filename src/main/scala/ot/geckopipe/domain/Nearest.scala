package ot.geckopipe.domain

case class Nearest(
    chr_id: String,
    position: Long,
    ref_allele: String,
    alt_allele: String,
    gene_id: String,
    d: Long,
    distance_score: Double,
    distance_score_q: Double,
    type_id: String,
    source_id: String,
    feature: String
)
