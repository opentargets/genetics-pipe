package ot.geckopipe.domain

case class LocusOverlap(
    A_study_id: String,
    A_chrom: String,
    A_pos: Long,
    A_ref: String,
    A_alt: String,
    B_study_id: String,
    B_chrom: String,
    B_pos: Long,
    B_ref: String,
    B_alt: String,
    A_distinct: Long,
    AB_overlap: Long,
    B_distinct: Long
)
