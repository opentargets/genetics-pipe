package ot.geckopipe.domain

case class IntervalDomain(
    chrom: String,
    start: Long,
    end: Long,
    gene_id: String, // TODO Became ensembl_id in recent version?
    score: Double,
    cell_type: String,
    // TODO There is no feature in recent version?
    feature: String
)
