package ot.geckopipe.domain

case class FineMapping(study_id: String,
                       lead_chrom: String,
                       lead_pos: Long,
                       lead_ref: String,
                       lead_alt: String,
                       tag_chrom: String,
                       tag_pos: Long,
                       tag_ref: String,
                       tag_alt: String,
                       log10_ABF: Option[Double] = None,
                       posterior_prob: Option[Double] = None)
