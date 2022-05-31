package ot.geckopipe.domain

case class Ld(
    study_id: String,
    lead_chrom: String,
    lead_pos: Long,
    lead_ref: String,
    lead_alt: String,
    tag_chrom: String,
    tag_pos: Long,
    tag_ref: String,
    tag_alt: String,
    overall_r2: Option[Double] = None,
    AFR_1000G_prop: Option[Double] = None,
    AMR_1000G_prop: Option[Double] = None,
    EAS_1000G_prop: Option[Double] = None,
    EUR_1000G_prop: Option[Double] = None,
    SAS_1000G_prop: Option[Double] = None,
    ld_available: Option[Boolean] = None
)
